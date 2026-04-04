"""
fix_connector.py — FIXConnector v1
TARE (Tick-Level Algorithmic Research Environment)

FIX protocol bridge to live brokers for order submission and execution reporting.

Правила:
  - Только int, никакого float
  - Никакой случайности
  - Детерминизм: одни входные данные → всегда одинаковый результат
  - Только стандартная библиотека Python
"""

from typing import Optional, Dict, Any
import hashlib
from io import StringIO


class FIXConnector:
    """
    FIX protocol bridge for live broker connectivity.
    
    Encodes outbound orders to FIX protocol format and decodes incoming
    execution messages deterministically. All integer arithmetic only.
    Maintains order mapping and execution history.
    """
    
    # FIX protocol constants
    FIX_VERSION = "FIX.4.2"
    SOH = "\x01"  # Start of Header delimiter
    
    # Message types
    MSG_TYPE_NEW_ORDER = "D"          # New Order Single
    MSG_TYPE_EXECUTION_REPORT = "8"   # Execution Report
    MSG_TYPE_ORDER_CANCEL = "F"       # Order Cancel Request
    
    # Order sides (FIX spec)
    SIDE_BUY = "1"
    SIDE_SELL = "2"
    
    # Order types (FIX spec)
    ORD_TYPE_MARKET = "1"
    ORD_TYPE_LIMIT = "2"
    
    # Order statuses (FIX spec)
    STATUS_NEW = "0"
    STATUS_PARTIALLY_FILLED = "1"
    STATUS_FILLED = "2"
    STATUS_CANCELLED = "4"
    STATUS_REJECTED = "8"
    
    def __init__(self, broker_config: dict) -> None:
        """
        Initialize FIX connector with broker configuration.
        
        Args:
            broker_config: Configuration dict with keys:
                - 'sender_comp_id' (str): Sender company ID
                - 'target_comp_id' (str): Target (broker) company ID
                - 'sender_sub_id' (str, optional): Sender sub ID
                - 'execution_engine' (ExecutionEngine, optional): Linked execution engine
                
        Raises:
            ValueError: If config is invalid or missing required fields.
            TypeError: If broker_config is not a dict.
        """
        if not isinstance(broker_config, dict):
            raise TypeError("broker_config must be a dict")
        
        if 'sender_comp_id' not in broker_config:
            raise ValueError("broker_config missing 'sender_comp_id'")
        
        if 'target_comp_id' not in broker_config:
            raise ValueError("broker_config missing 'target_comp_id'")
        
        sender_id = broker_config['sender_comp_id']
        target_id = broker_config['target_comp_id']
        
        if not isinstance(sender_id, str) or not sender_id:
            raise ValueError("sender_comp_id must be non-empty string")
        
        if not isinstance(target_id, str) or not target_id:
            raise ValueError("target_comp_id must be non-empty string")
        
        self._sender_comp_id = sender_id
        self._target_comp_id = target_id
        self._sender_sub_id = broker_config.get('sender_sub_id', '')
        self._execution_engine = broker_config.get('execution_engine', None)
        
        self._msg_seq_num = 0  # Deterministic sequence counter
        self._order_map = {}   # clordid -> {order_data}
        self._exec_reports = {}  # clordid -> execution_report
        self._checksum_enabled = broker_config.get('checksum_enabled', True)
    
    def _get_next_msg_seq_num(self) -> int:
        """
        Get next deterministic message sequence number.
        
        Increments internal counter and returns value. Deterministic
        across calls due to simple counter.
        
        Returns:
            int: Next message sequence number.
        """
        self._msg_seq_num += 1
        return self._msg_seq_num
    
    def _calculate_checksum(self, msg: str) -> str:
        """
        Calculate FIX message checksum (deterministic).
        
        Computes checksum as sum of ASCII values of all message bytes
        modulo 256, formatted as 3-digit zero-padded string per FIX spec.
        Deterministic: same message always produces same checksum.
        
        Args:
            msg: FIX message string (without checksum field).
            
        Returns:
            str: Checksum as 3-digit zero-padded string.
        """
        checksum = 0
        for char in msg:
            checksum = (checksum + ord(char)) % 256
        
        return str(checksum).zfill(3)
    
    def _build_fix_header(self, msg_type: str, body: str) -> str:
        """
        Build FIX message header (BeginString, BodyLength, MsgType).
        
        Args:
            msg_type: FIX message type code (e.g. 'D' for NewOrderSingle).
            body: Message body content (without header/trailer).
            
        Returns:
            str: Header string with field delimiters (SOH).
        """
        seq_num = self._get_next_msg_seq_num()
        
        # Build body with required fields
        body_with_type = f"35={msg_type}{self.SOH}" + body
        body_length = len(body_with_type.encode('utf-8'))
        
        # Header: BeginString, BodyLength
        header = f"8={self.FIX_VERSION}{self.SOH}9={body_length}{self.SOH}"
        
        return header + body_with_type
    
    def _append_fix_trailer(self, msg: str) -> str:
        """
        Append FIX message trailer (Checksum).
        
        Args:
            msg: FIX message without trailer.
            
        Returns:
            str: Complete FIX message with checksum trailer.
        """
        if self._checksum_enabled:
            checksum = self._calculate_checksum(msg)
            return msg + f"93={len(msg)}{self.SOH}10={checksum}{self.SOH}"
        else:
            return msg + f"93={len(msg)}{self.SOH}"
    
    def send_order(self, order: dict) -> str:
        """
        Encode and send order as FIX NewOrderSingle message.
        
        Converts order dict to FIX protocol message deterministically.
        Stores order for tracking. If execution_engine is available,
        submits order for simulation.
        
        Args:
            order: Order dict with keys:
                - 'symbol' (str): Asset symbol (e.g. 'EURUSD')
                - 'side' (str): 'BUY' or 'SELL'
                - 'quantity' (int): Order quantity (positive integer)
                - 'ord_type' (str): 'MARKET' or 'LIMIT'
                - 'price' (int, optional): Limit price for LIMIT orders
                - 'clordid' (str, optional): Client order ID
                
        Returns:
            str: FIX message string or error message.
            
        Raises:
            ValueError: If order parameters are invalid.
            TypeError: If order is not a dict.
        """
        if not isinstance(order, dict):
            raise TypeError("order must be a dict")
        
        # Validate order
        symbol = order.get('symbol', '')
        side = order.get('side', '')
        quantity = order.get('quantity', 0)
        ord_type = order.get('ord_type', '')
        
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("order['symbol'] must be non-empty string")
        
        if side not in ['BUY', 'SELL']:
            raise ValueError("order['side'] must be 'BUY' or 'SELL'")
        
        if not isinstance(quantity, int) or quantity <= 0:
            raise ValueError("order['quantity'] must be positive integer")
        
        if ord_type not in ['MARKET', 'LIMIT']:
            raise ValueError("order['ord_type'] must be 'MARKET' or 'LIMIT'")
        
        if ord_type == 'LIMIT' and 'price' not in order:
            raise ValueError("LIMIT orders must include 'price'")
        
        # Generate or use provided client order ID
        clordid = order.get('clordid', '')
        if not clordid:
            # Deterministic client order ID from order contents
            order_hash = hashlib.sha256(
                f"{symbol}_{side}_{quantity}_{ord_type}_{self._msg_seq_num}".encode()
            ).hexdigest()[:8].upper()
            clordid = f"ORD_{order_hash}"
        
        # Store order for tracking
        self._order_map[clordid] = {
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'ord_type': ord_type,
            'price': order.get('price', 0),
            'status': 'SUBMITTED'
        }
        
        # Convert to FIX side codes
        fix_side = self.SIDE_BUY if side == 'BUY' else self.SIDE_SELL
        fix_ord_type = self.ORD_TYPE_MARKET if ord_type == 'MARKET' else self.ORD_TYPE_LIMIT
        
        # Build message body
        body = f"49={self._sender_comp_id}{self.SOH}"
        if self._sender_sub_id:
            body += f"50={self._sender_sub_id}{self.SOH}"
        body += f"56={self._target_comp_id}{self.SOH}"
        body += f"34={self._get_next_msg_seq_num()}{self.SOH}"
        body += f"11={clordid}{self.SOH}"
        body += f"55={symbol}{self.SOH}"
        body += f"54={fix_side}{self.SOH}"
        body += f"38={quantity}{self.SOH}"
        body += f"40={fix_ord_type}{self.SOH}"
        
        if ord_type == 'LIMIT':
            price = order['price']
            body += f"44={price}{self.SOH}"
        
        # Build complete message
        msg = self._build_fix_header(self.MSG_TYPE_NEW_ORDER, body)
        fix_message = self._append_fix_trailer(msg)
        
        # Submit to execution engine if available
        if self._execution_engine is not None:
            try:
                order_id = self._execution_engine.submit_order(
                    symbol=symbol,
                    side=side,
                    size=quantity,
                    order_type=ord_type
                )
                self._order_map[clordid]['order_id'] = order_id
            except Exception as e:
                # Execution engine error, but still return FIX message
                pass
        
        return fix_message
    
    def receive_execution(self, msg: str) -> dict:
        """
        Decode and process FIX ExecutionReport message.
        
        Parses incoming FIX message deterministically, extracts execution
        data, and stores report for retrieval. If execution_engine is
        available, retrieves corresponding execution report.
        
        Args:
            msg: FIX ExecutionReport message string (SOH-delimited fields).
            
        Returns:
            dict: Execution report with keys:
                - 'clordid' (str): Client order ID
                - 'symbol' (str): Asset symbol
                - 'side' (str): 'BUY' or 'SELL'
                - 'order_qty' (int): Original order quantity
                - 'exec_qty' (int): Executed quantity
                - 'ord_status' (str): Order status code
                - 'status_text' (str): Human-readable status
                - 'exec_price' (int): Execution price (if available)
                - 'commission' (int): Commission in price units
                - 'latency_us' (int): Order latency microseconds
                - 'slippage_bps' (int): Slippage in basis points
                - 'raw_fields' (dict): All parsed FIX fields
                
            Empty dict if message cannot be parsed.
        """
        if not isinstance(msg, str):
            raise TypeError("msg must be a string")
        
        # Parse FIX message fields
        fields = self._parse_fix_message(msg)
        
        if not fields:
            return {}
        
        # Extract key fields
        clordid = fields.get('11', '')  # ClOrdID
        symbol = fields.get('55', '')   # Symbol
        side_code = fields.get('54', '') # Side
        order_qty = int(fields.get('38', '0'))  # OrderQty
        exec_qty = int(fields.get('32', '0'))   # ExecQty
        ord_status = fields.get('39', '')  # OrdStatus
        exec_price = int(fields.get('31', '0'))  # LastPx
        commission = int(fields.get('12', '0'))  # Commission
        
        # Convert side code to text
        side_text = 'BUY' if side_code == self.SIDE_BUY else 'SELL'
        
        # Convert status code to text
        status_map = {
            self.STATUS_NEW: 'NEW',
            self.STATUS_PARTIALLY_FILLED: 'PARTIALLY_FILLED',
            self.STATUS_FILLED: 'FILLED',
            self.STATUS_CANCELLED: 'CANCELLED',
            self.STATUS_REJECTED: 'REJECTED'
        }
        status_text = status_map.get(ord_status, 'UNKNOWN')
        
        # Retrieve execution engine report if available
        latency_us = 0
        slippage_bps = 0
        
        if clordid in self._order_map and self._execution_engine is not None:
            order_id = self._order_map[clordid].get('order_id', 0)
            if order_id > 0:
                try:
                    exec_report = self._execution_engine.get_execution_report(order_id)
                    latency_us = exec_report.get('latency_us', 0)
                    slippage_bps = exec_report.get('slippage_bps', 0)
                except Exception:
                    pass
        
        # Build execution report
        report = {
            'clordid': clordid,
            'symbol': symbol,
            'side': side_text,
            'order_qty': order_qty,
            'exec_qty': exec_qty,
            'ord_status': ord_status,
            'status_text': status_text,
            'exec_price': exec_price,
            'commission': commission,
            'latency_us': latency_us,
            'slippage_bps': slippage_bps,
            'raw_fields': fields.copy()
        }
        
        # Store report
        self._exec_reports[clordid] = report
        
        # Update order status
        if clordid in self._order_map:
            self._order_map[clordid]['status'] = status_text
        
        return report
    
    def _

"""
tests/test_tick_data_engine.py - Unit tests for TickDataEngine module.
"""

import csv
import struct
import tempfile
from pathlib import Path
import pytest

from tare.tick_data_engine import TickDataEngine


class TestTickDataEngineInit:
    """Test TickDataEngine initialization."""

    def test_init_default(self):
        """Test engine initialization with default jitter tolerance."""
        engine = TickDataEngine()
        assert engine is not None

    def test_init_custom_jitter(self):
        """Test engine initialization with custom jitter tolerance."""
        engine = TickDataEngine(max_jitter_us=500)
        assert engine is not None

    def test_init_invalid_jitter_negative(self):
        """Test engine rejects negative jitter tolerance."""
        with pytest.raises(ValueError):
            TickDataEngine(max_jitter_us=-100)

    def test_init_invalid_jitter_zero(self):
        """Test engine rejects zero jitter tolerance."""
        with pytest.raises(ValueError):
            TickDataEngine(max_jitter_us=0)

    def test_init_invalid_jitter_float(self):
        """Test engine rejects float jitter tolerance."""
        with pytest.raises(ValueError):
            TickDataEngine(max_jitter_us=100.5)


class TestTickDataEngineIngestCSV:
    """Test CSV ingestion."""

    @pytest.fixture
    def csv_file(self):
        """Create temporary CSV file with test data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'symbol', 'price', 'volume'])
            writer.writeheader()
            writer.writerow({
                'timestamp': '1000000000',
                'symbol': 'AAPL',
                'price': '15000',
                'volume': '100'
            })
            writer.writerow({
                'timestamp': '1000000100',
                'symbol': 'MSFT',
                'price': '30000',
                'volume': '50'
            })
            writer.writerow({
                'timestamp': '1000000000',
                'symbol': 'AAPL',
                'price': '15000',
                'volume': '100'
            })
            temp_path = f.name

        yield temp_path

        Path(temp_path).unlink()

    def test_ingest_csv_success(self, csv_file):
        """Test successful CSV ingestion."""
        engine = TickDataEngine()
        engine.ingest(csv_file, 'csv')

        ticks = engine.get_clean_ticks()
        assert len(ticks) == 2  # Duplicate removed
        assert all(isinstance(t['timestamp'], int) for t in ticks)
        assert all(isinstance(t['symbol'], str) for t in ticks)
        assert all(isinstance(t['price'], int) for t in ticks)
        assert all(isinstance(t['volume'], int) for t in ticks)

    def test_ingest_csv_removes_duplicates(self, csv_file):
        """Test that CSV ingestion removes duplicates."""
        engine = TickDataEngine()
        engine.ingest(csv_file, 'csv')

        ticks = engine.get_clean_ticks()
        # Should have 2 ticks (1 duplicate removed)
        assert len(ticks) == 2

    def test_ingest_csv_standardizes_data(self, csv_file):
        """Test that CSV ingestion standardizes data."""
        engine = TickDataEngine()
        engine.ingest(csv_file, 'csv')

        ticks = engine.get_clean_ticks()
        # Check all required fields present
        for tick in ticks:
            assert 'timestamp' in tick
            assert 'symbol' in tick
            assert 'price' in tick
            assert 'volume' in tick

    def test_ingest_csv_file_not_found(self):
        """Test ingestion fails for missing file."""
        engine = TickDataEngine()
        with pytest.raises(FileNotFoundError):
            engine.ingest('/nonexistent/file.csv', 'csv')

    def test_ingest_invalid_source_type(self):
        """Test ingestion rejects non-string source."""
        engine = TickDataEngine()
        with pytest.raises(ValueError):
            engine.ingest(123, 'csv')

    def test_ingest_invalid_format_type(self):
        """Test ingestion rejects non-string format."""
        engine = TickDataEngine()
        with pytest.raises(ValueError):
            engine.ingest('test.csv', 123)

    def test_ingest_unsupported_format(self):
        """Test ingestion rejects unsupported format."""
        engine = TickDataEngine()
        with pytest.raises(ValueError):
            engine.ingest('test.csv', 'unknown')


class TestTickDataEngineIngestBinary:
    """Test binary ingestion."""

    @pytest.fixture
    def binary_file(self):
        """Create temporary binary file with test data."""
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.bin', delete=False) as f:
            # Tick 1: timestamp=1000000000, symbol="AAPL", price=15000, volume=100
            tick1 = struct.pack('>QBBBBqq',
                               1000000000,
                               ord('A'), ord('A'), ord('P'), ord('L'),
                               15000, 100)
            f.write(tick1)

            # Tick 2: timestamp=1000000100, symbol="MSFT", price=30000, volume=50
            tick2 = struct.pack('>QBBBBqq',
                               1000000100,
                               ord('M'), ord('S'), ord('F'), ord('T'),
                               30000, 50)
            f.write(tick2)

            temp_path = f.name

        yield temp_path

        Path(temp_path).unlink()

    def test_ingest_binary_success(self, binary_file):
        """Test successful binary ingestion."""
        engine = TickDataEngine()
        engine.ingest(binary_file, 'binary')

        ticks = engine.get_clean_ticks()
        assert len(ticks) == 2
        assert all(isinstance(t['timestamp'], int) for t in ticks)
        assert all(isinstance(t['symbol'], str) for t in ticks)

    def test_ingest_binary_file_not_found(self):
        """Test binary ingestion fails for missing file."""
        engine = TickDataEngine()
        with pytest.raises(FileNotFoundError):
            engine.ingest('/nonexistent/file.bin', 'binary')


class TestTickDataEngineGetCleanTicks:
    """Test retrieving clean ticks."""

    @pytest.fixture
    def ingested_engine(self):
        """Create engine with ingested data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'symbol', 'price', 'volume'])
            writer.writeheader()
            writer.writerow({
                'timestamp': '1000000000',
                'symbol': 'AAPL',
                'price': '15000',
                'volume': '100'
            })
            temp_path = f.name

        engine = TickDataEngine()
        engine.ingest(temp_path, 'csv')

        yield engine

        Path(temp_path).unlink()

    def test_get_clean_ticks_success(self, ingested_engine):
        """Test successful retrieval of clean ticks."""
        ticks = ingested_engine.get_clean_ticks()
        assert len(ticks) == 1
        assert ticks[0]['symbol'] == 'AAPL'
        assert ticks[0]['price'] == 15000

    def test_get_clean_ticks_empty(self):
        """Test get_clean_ticks fails when no data ingested."""
        engine = TickDataEngine()
        with pytest.raises(ValueError):
            engine.get_clean_ticks()

    def test_get_clean_ticks_returns_copy(self, ingested_engine):
        """Test that get_clean_ticks returns a copy."""
        ticks1 = ingested_engine.get_clean_ticks()
        ticks2 = ingested_engine.get_clean_ticks()

        assert ticks1 == ticks2
        assert ticks1 is not ticks2


class TestTickDataEngineCorrectJitter:
    """Test jitter correction."""

    @pytest.fixture
    def ingested_engine(self):
        """Create engine with ingested data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'symbol', 'price', 'volume'])
            writer.writeheader()
            writer.writerow({
                'timestamp': '1000000000',
                'symbol': 'AAPL',
                'price': '15000',
                'volume': '100'
            })
            writer.writerow({
                'timestamp': '1000000100',
                'symbol': 'AAPL',
                'price': '15100',
                'volume': '50'
            })
            temp_path = f.name

        engine = TickDataEngine()
        engine.ingest(temp_path, 'csv')

        yield engine

        Path(temp_path).unlink()

    def test_correct_jitter_success(self, ingested_engine):
        """Test successful jitter correction."""
        ingested_engine.correct_jitter()
        ticks = ingested_engine.get_clean_ticks()
        assert len(ticks) == 2
        assert all(isinstance(t['timestamp'], int) for t in ticks)

    def test_correct_jitter_no_data(self):
        """Test jitter correction fails with no data."""
        engine = TickDataEngine()
        with pytest.raises(ValueError):
            engine.correct_jitter()

    def test_correct_jitter_invalid_max_jitter(self, ingested_engine):
        """Test jitter correction rejects invalid max_jitter_us."""
        with pytest.raises(ValueError):
            ingested_engine.correct_jitter(max_jitter_us=-100)


class TestTickDataEngineSynchronizeSources:
    """Test multi-source synchronization."""

    @pytest.fixture
    def source_files(self):
        """Create temporary CSV files for multiple sources."""
        files = {}

        # Source 1
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'symbol', 'price', 'volume'])
            writer.writeheader()
            writer.writerow({'timestamp': '1000000200', 'symbol': 'AAPL', 'price': '15000', 'volume': '100'})
            writer.writerow({'timestamp': '1000000000', 'symbol': 'AAPL', 'price': '14900', 'volume': '50'})
            files['source1'] = f.name

        # Source 2
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'symbol', 'price', 'volume'])
            writer.writeheader()
            writer.writerow({'timestamp': '1000000100', 'symbol': 'AAPL', 'price': '15000', 'volume': '75'})
            writer.writerow({'timestamp': '1000000300', 'symbol': 'AAPL', 'price': '15100', 'volume': '25'})
            files['source2'] = f.name

        yield files

        for path in files.values():
            Path(path).unlink()

    def test_synchronize_sources_success(self, source_files):
        """Test successful multi-source synchronization."""
        engine = TickDataEngine()
        engine.synchronize_sources(source_files, 'csv')

        ticks = engine.get_clean_ticks()
        assert len(ticks) == 4
        assert all('source' in t for t in ticks)

        # Check sorted by market_time (synchronize renames timestamp → market_time)
        key = 'market_time' if 'market_time' in ticks[0] else 'timestamp'
        timestamps = [t[key] for t in ticks]
        assert timestamps == sorted(timestamps)

    def test_synchronize_sources_invalid_sources_type(self):
        """Test synchronize_sources rejects non-dict sources."""
        engine = TickDataEngine()
        with pytest.raises(ValueError):
            engine.synchronize_sources('not_a_dict', 'csv')

    def test_synchronize_sources_invalid_format(self, source_files):
        """Test synchronize_sources rejects unsupported format."""
        engine = TickDataEngine()
        with pytest.raises(ValueError):
            engine.synchronize_sources(source_files, 'unknown')


class TestTickDataEngineExportSnapshot:
    """Test snapshot export."""

    @pytest.fixture
    def ingested_engine(self):
        """Create engine with ingested data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'symbol', 'price', 'volume'])
            writer.writeheader()
            writer.writerow({
                'timestamp': '1000000000',
                'symbol': 'AAPL',
                'price': '15000',
                'volume': '100'
            })
            writer.writerow({
                'timestamp': '1000000100',
                'symbol': 'MSFT',
                'price': '30000',
                'volume': '50'
            })
            temp_path = f.name

        engine = TickDataEngine()
        engine.ingest(temp_path, 'csv')

        yield engine

        Path(temp_path).unlink()

    def test_export_snapshot_success(self, ingested_engine):
        """Test successful snapshot export."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            export_path = f.name

        try:
            ingested_engine.export_snapshot(export_path)

            # Verify exported file
            exported_ticks = []
            with open(export_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    exported_ticks.append({
                        'timestamp': int(row['timestamp']),
                        'symbol': row['symbol'],
                        'price': int(row['price']),
                        'volume': int(row['volume'])
                    })

            assert len(exported_ticks) == 2

        finally:
            Path(export_path).unlink()

    def test_export_snapshot_no_data(self):
        """Test export fails with no data."""
        engine = TickDataEngine()
        with pytest.raises(ValueError):
            engine.export_snapshot('/tmp/test.csv')

    def test_export_snapshot_invalid_path_type(self, ingested_engine):
        """Test export rejects non-string path."""
        with pytest.raises(ValueError):
            ingested_engine.export_snapshot(123)


class TestTickDataEngineStreamCleanTicks:
    """Test streaming clean ticks."""

    @pytest.fixture
    def ingested_engine(self):
        """Create engine with ingested data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'symbol', 'price', 'volume'])
            writer.writeheader()
            for i in range(5):
                writer.writerow({
                    'timestamp': str(1000000000 + i * 100),
                    'symbol': 'AAPL',
                    'price': str(15000 + i),
                    'volume': '100'
                })
            temp_path = f.name

        engine = TickDataEngine()
        engine.ingest(temp_path, 'csv')

        yield engine

        Path(temp_path).unlink()

    def test_stream_clean_ticks_success(self, ingested_engine):
        """Test successful streaming of clean ticks."""
        count = 0
        for tick in ingested_engine.stream_clean_ticks():
            assert isinstance(tick, dict)
            assert 'timestamp' in tick
            count += 1

        assert count == 5

    def test_stream_clean_ticks_no_data(self):
        """Test streaming fails with no data."""
        engine = TickDataEngine()
        with pytest.raises(ValueError):
            for _ in engine.stream_clean_ticks():
                pass


class TestTickDataEngineGetStats:
    """Test statistics retrieval."""

    @pytest.fixture
    def ingested_engine(self):
        """Create engine with ingested data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'symbol', 'price', 'volume'])
            writer.writeheader()
            writer.writerow({'timestamp': '1000000000', 'symbol': 'AAPL', 'price': '15000', 'volume': '100'})
            writer.writerow({'timestamp': '1000000100', 'symbol': 'MSFT', 'price': '30000', 'volume': '50'})
            writer.writerow({'timestamp': '1000000000', 'symbol': 'AAPL', 'price': '15000', 'volume': '100'})  # dup
            temp_path = f.name

        engine = TickDataEngine()
        engine.ingest(temp_path, 'csv')

        yield engine

        Path(temp_path).unlink()

    def test_get_stats_success(self, ingested_engine):
        """Test successful stats retrieval."""
        stats = ingested_engine.get_stats()

        assert 'raw_count' in stats
        assert 'clean_count' in stats
        assert 'unique_symbols' in stats
        assert 'timestamp_range' in stats

        assert stats['raw_count'] == 3
        assert stats['clean_count'] == 2
        assert stats['unique_symbols'] == 2
        assert isinstance(stats['timestamp_range'], tuple)
        assert len(stats['timestamp_range']) == 2

    def test_get_stats_no_data(self):
        """Test stats retrieval fails with no data."""
        engine = TickDataEngine()
        with pytest.raises(ValueError):
            engine.get_stats()


class TestTickDataEngineDeterminism:
    """Test deterministic behavior."""

    @pytest.fixture
    def csv_file(self):
        """Create temporary CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'symbol', 'price', 'volume'])
            writer.writeheader()
            for i in range(10):
                writer.writerow({
                    'timestamp': str(1000000000 + i * 100),
                    'symbol': 'AAPL',
                    'price': str(15000 + i),
                    'volume': '100'
                })
            temp_path = f.name

        yield temp_path

        Path(temp_path).unlink()

    def test_deterministic_ingestion(self, csv_file):
        """Test that identical ingestion produces identical results."""
        engine1 = TickDataEngine()
        engine1.ingest(csv_file, 'csv')
        ticks1 = engine1.get_clean_ticks()

        engine2 = TickDataEngine()
        engine2.ingest(csv_file, 'csv')
        ticks2 = engine2.get_clean_ticks()

        assert ticks1 == ticks2

    def test_deterministic_jitter_correction(self, csv_file):
        """Test that jitter correction is deterministic."""
        engine1 = TickDataEngine()
        engine1.ingest(csv_file, 'csv')
        engine1.correct_jitter()
        ticks1 = engine1.get_clean_ticks()

        engine2 = TickDataEngine()
        engine2.ingest(csv_file, 'csv')
        engine2.correct_jitter()
        ticks2 = engine2.get_clean_ticks()

        assert ticks1 == ticks2


class TestTickDataEngineTypeConstraints:
    """Test type constraints (no float, all int)."""

    @pytest.fixture
    def ingested_engine(self):
        """Create engine with ingested data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'symbol', 'price', 'volume'])
            writer.writeheader()
            writer.writerow({
                'timestamp': '1000000000',
                'symbol': 'AAPL',
                'price': '15000',
                'volume': '100'
            })
            temp_path = f.name

        engine = TickDataEngine()
        engine.ingest(temp_path, 'csv')

        yield engine

        Path(temp_path).unlink()

    def test_no_float_in_output(self, ingested_engine):
        """Test that output contains no float values."""
        ticks = ingested_engine.get_clean_ticks()

        for tick in ticks:
            for key, value in tick.items():
                assert not isinstance(value, float), f"Found float in {key}"

    def test_all_numeric_fields_are_int(self, ingested_engine):
        """Test that numeric fields are int type."""
        ticks = ingested_engine.get_clean_ticks()

        for tick in ticks:
            assert isinstance(tick['timestamp'], int)
            assert isinstance(tick['price'], int)
            assert isinstance(tick['volume'], int)

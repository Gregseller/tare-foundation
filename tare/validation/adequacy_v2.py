"""
adequacy_v2.py - AdequacyV2 Extended Validator for TARE.
Phase: 5
"""
from tare.validation.adequacy_v1 import AdequacyV1


class AdequacyV2:
    """Extended validation with KS tests and FX adequacy checks."""

    def __init__(self, adequacy_v1=None):
        self._adequacy_v1 = adequacy_v1
        self._violations = []

    def ks_test(self, sample1: list, sample2: list) -> dict:
        if not isinstance(sample1, list) or not isinstance(sample2, list):
            raise ValueError("sample1 and sample2 must be lists")
        if not sample1 or not sample2:
            raise ValueError("lists cannot be empty")
        for v in sample1:
            if not isinstance(v, int):
                raise ValueError("sample1 must contain only int")
        for v in sample2:
            if not isinstance(v, int):
                raise ValueError("sample2 must contain only int")

        s1 = sorted(sample1)
        s2 = sorted(sample2)
        n1, n2 = len(s1), len(s2)

        all_vals = sorted(set(s1 + s2))
        max_diff = 0
        for val in all_vals:
            cdf1 = sum(1 for x in s1 if x <= val) * 10000 // n1
            cdf2 = sum(1 for x in s2 if x <= val) * 10000 // n2
            diff = abs(cdf1 - cdf2)
            if diff > max_diff:
                max_diff = diff

        return {"statistic": max_diff, "adequate": max_diff == 0}

    def fx_adequacy_check(self, fx_pairs: dict) -> dict:
        if not isinstance(fx_pairs, dict):
            raise ValueError("fx_pairs must be a dict")
        if not fx_pairs:
            return {}

        # Валидация структуры
        for symbol, data in fx_pairs.items():
            if not isinstance(data, dict):
                raise ValueError(f"{symbol}: data must be dict")
            for key in ("bids", "asks", "volumes"):
                if key not in data:
                    raise ValueError(f"{symbol}: missing key {key}")
                for val in data[key]:
                    if not isinstance(val, int):
                        raise ValueError(f"{symbol}.{key}: must contain only int")

        result = {}
        for symbol, data in fx_pairs.items():
            bids = data["bids"]
            asks = data["asks"]
            spreads_ok = all(a > b for a, b in zip(asks, bids))
            volumes_ok = all(v >= 0 for v in data["volumes"])
            result[symbol] = spreads_ok and volumes_ok

        return result

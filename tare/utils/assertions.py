"""
assertions.py — Детерминистические проверки инвариантов.

Используется во всех модулях для контроля корректности данных.
"""


def assert_int(value, name: str = "value") -> None:
    """Гарантирует что value — int. Иначе TypeError."""
    if not isinstance(value, int):
        raise TypeError(f"{name} должен быть int, получен {type(value).__name__}: {value!r}")


def assert_positive(value: int, name: str = "value") -> None:
    """Гарантирует что value > 0."""
    assert_int(value, name)
    if value <= 0:
        raise ValueError(f"{name} должен быть > 0, получен: {value}")


def assert_non_negative(value: int, name: str = "value") -> None:
    """Гарантирует что value >= 0."""
    assert_int(value, name)
    if value < 0:
        raise ValueError(f"{name} должен быть >= 0, получен: {value}")

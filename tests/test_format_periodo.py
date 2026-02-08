from datetime import date

from extrai_dados_financeiros_brapi import format_periodo


def test_format_periodo_quarters():
    assert format_periodo(date(2025, 9, 30)) == "3T2025"
    assert format_periodo(date(2025, 6, 30)) == "2T2025"
    assert format_periodo(date(2025, 3, 31)) == "1T2025"
    assert format_periodo(date(2024, 12, 31)) == "4T2024"


def test_format_periodo_fallback():
    assert format_periodo("2024-07-01") == "2024-07"
    assert format_periodo(None) == "N/A"

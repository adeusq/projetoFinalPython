"""Testes unitários do extrator da API de contratos (sem chamadas de rede reais)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.extractors import api_extractor


def test_to_api_date_converts_iso_to_ddmmyyyy():
    assert api_extractor._to_api_date("2026-01-05") == "05/01/2026"


def _fake_response(total_pages: int, page: int) -> dict:
    return {
        "sumary": {"total_pages": total_pages, "current_page": str(page), "total_records": total_pages * 100},
        "data": [{"id": page * 1000 + i} for i in range(2)],
    }


@patch("src.extractors.api_extractor.write_text")
@patch("src.extractors.api_extractor.requests.get")
def test_run_extraction_paginates_and_writes_each_page(mock_get, mock_write_text):
    def side_effect(url, params, timeout):
        page = params["page"]
        response = MagicMock()
        response.json.return_value = _fake_response(total_pages=3, page=page)
        response.raise_for_status.return_value = None
        return response

    mock_get.side_effect = side_effect

    result = api_extractor.run_extraction("2026-01-01", "2026-06-30", run_id="20260101")

    assert result["total_pages_available"] == 3
    assert result["pages_fetched"] == 3
    assert result["rows"] == 6  # 2 registros x 3 páginas
    assert mock_write_text.call_count == 3
    # A primeira página não deve ser buscada duas vezes (reaproveita a chamada usada para descobrir total_pages)
    assert mock_get.call_count == 3


@patch("src.extractors.api_extractor.write_text")
@patch("src.extractors.api_extractor.requests.get")
def test_run_extraction_respects_max_pages(mock_get, mock_write_text):
    def side_effect(url, params, timeout):
        response = MagicMock()
        response.json.return_value = _fake_response(total_pages=10, page=params["page"])
        response.raise_for_status.return_value = None
        return response

    mock_get.side_effect = side_effect

    result = api_extractor.run_extraction("2026-01-01", "2026-06-30", run_id="20260101", max_pages=2)

    assert result["total_pages_available"] == 10
    assert result["pages_fetched"] == 2
    assert mock_write_text.call_count == 2

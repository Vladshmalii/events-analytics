from unittest.mock import MagicMock, patch

import pytest

from app.db.clickhouse import query_with_filter

EVIL = "x' OR '1'='1' --"


@pytest.mark.parametrize("segment,key_param", [
    (f"event_type:{EVIL}", None),
    (f"properties.plan:{EVIL}", "plan"),
])
def test_malicious_segment_is_bound_not_interpolated(segment, key_param):
    client = MagicMock()
    client.query.return_value.result_rows = []
    with patch("app.db.clickhouse.get_client", return_value=client):
        query_with_filter("2025-01-01", "2025-01-02", segment)

    sql = client.query.call_args.args[0]
    params = client.query.call_args.kwargs["parameters"]
    assert "1'='1" not in sql
    assert params["seg_value"] == EVIL
    if key_param:
        assert params["seg_key"] == key_param


def test_malicious_property_key_is_bound():
    client = MagicMock()
    client.query.return_value.result_rows = []
    with patch("app.db.clickhouse.get_client", return_value=client):
        query_with_filter("2025-01-01", "2025-01-02", "properties.a') OR 1=1 --:v")
    assert "OR 1=1" not in client.query.call_args.args[0]


@pytest.mark.parametrize("segment", ["nocolon", "unknown:v"])
def test_invalid_segment_rejected(segment):
    with patch("app.db.clickhouse.get_client", return_value=MagicMock()):
        with pytest.raises(ValueError):
            query_with_filter("2025-01-01", "2025-01-02", segment)

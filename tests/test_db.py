from sst_engine.sst_engine import DB
import pytest


def test_ops_without_exceeding_capacity():
    db = DB()
    db["foo"] = "bar"
    assert db["foo"] == "bar"


def test_deletion():
    db = DB()
    db["foo"] = "bar"
    del db["foo"]
    with pytest.raises(Exception):
        _ = db["foo"]

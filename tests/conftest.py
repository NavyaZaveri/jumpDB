from sst_engine import make_new_segment
import pytest


@pytest.fixture()
def segment():
    return make_new_segment()

import pytest
from pyspark_demo.utils import math

def test_add():
    assert(math.add(2,3), 5)

import pytest
from pyspark_demo.utils import math_ops


def test_add():
    assert (math_ops.add(2, 3), 5)

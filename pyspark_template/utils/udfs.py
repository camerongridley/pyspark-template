from pyspark.sql import DataFrame


def mult_col(x, factor: int) -> int:
    """
    Multiply a column by a factor
    :param x: value at column
    :param factor: factor to multiply by
    :return: x * factor
    """
    return x * factor


def handle_null_example(x, n: int) -> int:
    """
    This is to demonstrate that with UDFs, if a col has null, in its value, the UDF is never called
    and the row for that col will stay null.
    So it doesn't matter if you try and handle null/None.
    :param x: value at column
    :param n: int to add
    :return: col val plus n
    """
    if x is None:
        return 100
    else:
        return x + n



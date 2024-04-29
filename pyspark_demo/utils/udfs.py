from pyspark.sql import DataFrame


def mult_col(x, factor: int) -> int:
    return x * factor

'''
This is to demonstrate that with UDFs, if a col has null, in it's value, the UDF is never called
and the row for that col will stay null.
So it doesn't matter if you try and handle null/None.
'''
def handle_null_example(x, n: int) -> int:
    if x is None:
        return 100
    else:
        return x + n



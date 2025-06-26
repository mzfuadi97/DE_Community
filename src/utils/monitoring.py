import time
import logging

def measure_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        logging.info(f"Execution time for {func.__name__}: {execution_time:.2f} seconds")
        return result
    return wrapper

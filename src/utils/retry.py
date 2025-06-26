import time
import logging

def retry_operation(func, retries: int = 3, delay: int = 5):
    for _ in range(retries):
        try:
            return func()
        except Exception as e:
            logging.error(f"Error: {e}, retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Operation failed after multiple retries")

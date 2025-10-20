import time

# import logging
from functools import wraps

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


def throttle(interval):
    """
    Decorator to limit how often a function can be called.
    """

    def decorator(func):
        last_run_time = 0

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal last_run_time
            current_time = time.time()
            if current_time - last_run_time >= interval:
                last_run_time = current_time
                return func(*args, **kwargs)
            # If the interval has not passed, do nothing.

        return wrapper

    return decorator


@throttle(5)  # Apply decorator to print at most once every 5 seconds
def log_progress(message):
    """This function's calls will be throttled."""
    print(message)

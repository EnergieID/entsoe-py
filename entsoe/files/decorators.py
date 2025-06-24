from functools import wraps
import pandas as pd


def check_expired(func):
    @wraps(func)
    def check_expired_wrapper(*args, **kwargs):
        self = args[0]

        if pd.Timestamp.now(tz='europe/amsterdam') >= self.expire:
            self._update_token()

        return func(*args, **kwargs)

    return check_expired_wrapper

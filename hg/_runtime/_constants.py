from datetime import datetime, timedelta

__all__ = ("MIN_TD", "MIN_DT", "MAX_DT", "MIN_ST", "MAX_ET")


MIN_TD = timedelta(microseconds=1)  # The smallest engine time increment
MIN_DT = datetime(1970, 1, 1, 0, 0, 0, 0)  # The smallest engine time
MAX_DT = datetime(2300, 1, 1, 0, 0, 0, 0)  # The largest engine time
MIN_ST = MIN_DT+MIN_TD  # The smallest engine start time
MAX_ET = MAX_DT-MIN_TD  # The largest engine end time

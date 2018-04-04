import os

BINANCE_TIME_WINDOW_DICT = {"1min": "1m", "5min": "5m", "15min": "15m", "30min": "30m", "60min": "1h",
                            "1hour": "1h", "1day": "1d", "1mon": "1M", "1week": "1w"}

def create_dir_if_not_exist(path):
    if not os.path.exists(path):
        os.makedirs(path)


def get_seconds_from_time_window(time_window):
    second_per_minutes = 60
    second_per_hours = 60 * second_per_minutes
    second_per_day = 24 * second_per_hours

    if time_window == '1min':
        return 1 * second_per_minutes
    elif time_window == '5min':
        return 5 * second_per_minutes
    elif time_window == '15min':
        return 15 * second_per_minutes
    elif time_window == '30min':
        return 30 * second_per_minutes
    elif time_window == '60min' or time_window == '1hour':
        return 1 * second_per_hours
    elif time_window == '1day':
        return second_per_day
    elif time_window == '1mon':
        return 30 * second_per_day
    elif time_window == '1week':
        return 7 * second_per_day

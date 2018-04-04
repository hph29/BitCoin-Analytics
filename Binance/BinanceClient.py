import time
from datetime import datetime

from Utilities import Utils
import os
from requests import exceptions as HTTPExceptions

from Binance.client import Client
from Utilities import Logger as log


class BinanceClient:

    BINANCE_TIME_WINDOW_DICT = {"1min": "1m", "5min": "5m", "15min": "15m", "30min": "30m", "60min": "1h",
                                "1hour": "1h", "1day": "1d", "1mon": "1M", "1week": "1w"}

    def __init__(self, secret_key, public_key, parent_dir, time_window):
        self.client = Client(secret_key, public_key)
        self.logger = log.Logger("[BINANCE]")
        self.parent_dir = parent_dir
        self.time_window = time_window

    def __pull_data_from_binance(self, root_dir, time_window):
        symbols = self.__get_all_symbols()
        for symbol in symbols:
            kline_json = self.__get_response(symbol, time_window)
            if self.__is_valid_response(kline_json):
                self.__append_to_file(root_dir, kline_json, symbol)

    def __append_to_file(self, root_dir, json, symbol):
        date = self.__format_date_from_timestamp()
        path = os.path.join(root_dir, 'BINANCE', date)
        Utils.create_dir_if_not_exist(path)
        file_path = os.path.join(path, symbol.upper())
        with open(file_path, 'a+') as file:
            file.write(str(json[0]) + '\n')
            self.logger.debug("Appended data for symbol %s at %s" % (symbol, file_path))

    def __get_response(self, symbol, time_window, current_try=0):
        time_window = self.BINANCE_TIME_WINDOW_DICT[time_window]
        if current_try < 5:
            try:
                return self.client.get_klines(symbol=symbol, interval=time_window, limit=1)
            except HTTPExceptions.ReadTimeout:
                self.logger.info("ReadTimeOut for symbol %s, retrying..." % symbol)
                current_try += 1
                return self.__get_response(symbol, time_window, current_try)
        else:
            self.logger.warn("ReadTimeOut retry for symbol %s reached limit, abort.")

    def __get_all_symbols(self):
        try:
            json_content = self.client.get_all_tickers()
            return [json_content[i]['symbol'] for i in range(len(json_content))]
        except HTTPExceptions.ReadTimeout:
            self.logger.info("ReadTimeOut for getting all symbols, retrying...")
            time.sleep(1)
            self.__get_all_symbols()

    @staticmethod
    def __format_date_from_timestamp():
        return datetime.now().strftime('%Y%m%d')

    @staticmethod
    def __is_valid_response(json):
        return json is not None and json[0] is not None

    @staticmethod
    def __format_symbol(from_symbol, to_symbol):
        return ("%s%s" % (from_symbol, to_symbol)).lower()

    def run(self):
        while True:
            try:
                self.__pull_data_from_binance(self.parent_dir, self.time_window)
            except Exception as e:
                self.logger.error("Unexpected Exception: " + str(e))
            time.sleep(Utils.get_seconds_from_time_window(self.time_window))



if __name__ == '__main__':
    BinanceClient("", "", '', '1min').run()

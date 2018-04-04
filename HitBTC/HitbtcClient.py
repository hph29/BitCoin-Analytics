import time

from Utilities import Utils
import os
from dateutil import parser as DateParser
from requests import exceptions as HTTPExceptions

from HitBTC.Client import Client
from Utilities import Logger as log


class HitbtcClient:

    HITBTC_TIME_WINDOW_DICT = {"1min": "M1", "5min": "M5", "15min": "M15", "30min": "M30", "60min": "H1",
                               "1hour": "H1", "1day": "D1", "1mon": "1M", "1week": "D7"}

    def __init__(self, secret_key, public_key, parent_dir, time_window):
        self.client = Client(secret_key, public_key)
        self.logger = log.Logger("[HITBTC]")
        self.parent_dir = parent_dir
        self.time_window = time_window

    def __pull_data_from_hitbtc(self, root_dir, time_window):
        symbols = self.__get_all_symbols()
        time_window = self.HITBTC_TIME_WINDOW_DICT[time_window]
        for symbol in symbols:
            kline_json = self.__get_response(symbol, time_window)
            if self.__is_valid_response(kline_json):
                self.__append_to_file(root_dir, kline_json, symbol)

    def __append_to_file(self, root_dir, json, symbol):
        timestamp = json[0]['timestamp']
        date = self.__format_date_from_timestamp(timestamp)
        path = os.path.join(root_dir, 'HITBTC', date)
        Utils.create_dir_if_not_exist(path)
        file_path = os.path.join(path, symbol.upper())
        with open(file_path, 'a+') as file:
            file.write(str(json[0]) + '\n')
            self.logger.debug("Appended data for symbol %s at %s" % (symbol, file_path))

    def __get_response(self, symbol, time_window, current_try=0):
        if current_try < 5:
            try:
                return self.client.get_candles(symbol, time_window, 1)
            except HTTPExceptions.ReadTimeout:
                self.logger.info("ReadTimeOut for symbol %s, retrying..." % symbol)
                current_try += 1
                return self.__get_response(symbol, time_window, current_try)
        else:
            self.logger.warn("ReadTimeOut retry for symbol %s reached limit, abort.")

    def __get_all_symbols(self):
        try:
            json_content = self.client.get_all_symbols()
            return [json_content[i]['id'] for i in range(len(json_content))]
        except HTTPExceptions.ReadTimeout:
            self.logger.info("ReadTimeOut for getting all symbols, retrying...")
            time.sleep(1)
            self.__get_all_symbols()

    @staticmethod
    def __format_date_from_timestamp(timestamp):
        return DateParser.parse(timestamp).strftime('%Y%m%d')

    @staticmethod
    def __is_valid_response(json):
        return json is not None and json[0] is not None

    @staticmethod
    def __format_symbol(from_symbol, to_symbol):
        return ("%s%s" % (from_symbol, to_symbol)).lower()

    def run(self):
        while True:
            try:
                self.__pull_data_from_hitbtc(self.parent_dir, self.time_window)
            except Exception as e:
                self.logger.error("Unexpected Exception: " + str(e))
            time.sleep(Utils.get_seconds_from_time_window(self.time_window))

if __name__ == '__main__':
    HitbtcClient("", "", '', '1min').run()

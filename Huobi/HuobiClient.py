import time
from datetime import datetime

from Utilities import Utils
import os
from requests import exceptions as HTTPExceptions

from Huobi import Client
from Huobi import Utils as Huobi_Connection_Utils
from Utilities import Logger as log


class HuobiClient:

    HUOBI_TIME_WINDOW_DICT = {"1min": "1min", "5min": "5min", "15min": "15min", "30min": "30min", "60min": "60min",
                              "1hour": "60min", "1day": "1day", "1mon": "1mon", "1week": "1week"}

    def __init__(self, secret_key, public_key, parent_dir, time_window):
        Huobi_Connection_Utils.SECRET_KEY = secret_key
        Huobi_Connection_Utils.ACCESS_KEY = public_key
        self.client = Client
        self.logger = log.Logger("[HUOBI]")
        self.parent_dir = parent_dir
        self.time_window = self.HUOBI_TIME_WINDOW_DICT[time_window]

    def __pull_data_from_huobi(self, root_dir, time_window):
        symbols = self.__get_all_symbols()
        if symbols:
            for symbol in symbols:
                kline_json = self.__get_response(symbol, time_window)
                if self.__is_valid_response(kline_json):
                    self.__append_to_file(root_dir, kline_json, symbol)

    def __append_to_file(self, root_dir, json, symbol):
        timestamp = json['data'][0]['id']
        date = self.__format_date_from_timestamp(timestamp)
        path = os.path.join(root_dir, 'HUOBI', date)
        Utils.create_dir_if_not_exist(path)
        file_path = os.path.join(path, symbol.upper())
        with open(file_path, 'a+') as file:
            file.write(str(json['data'][0]) + '\n')
            self.logger.debug("Appended data for symbol %s at %s" % (symbol, file_path))

    def __get_response(self, symbol, time_window, current_try=0):
        if current_try < 5:
            try:
                return self.client.get_kline(symbol, time_window, 1)
            except HTTPExceptions.ReadTimeout:
                self.logger.info("ReadTimeOut for symbol %s, retrying..." % symbol)
                current_try += 1
                return self.__get_response(symbol, time_window, current_try)
        else:
            self.logger.warn("ReadTimeOut retry for symbol %s reached limit, abort.")

    def __get_all_symbols(self):
        try:
            json_content = self.client.get_symbols()
            return [self.__format_symbol(json_content['data'][i]['base-currency'], json_content['data'][i]['quote-currency'])
                    for i in range(len(json_content['data']))]
        except HTTPExceptions.ReadTimeout:
            self.logger.info("ReadTimeOut for getting all symbols, retrying...")
            time.sleep(1)
            self.__get_all_symbols()


    @staticmethod
    def __format_date_from_timestamp(timestamp):
        return datetime.fromtimestamp(int(timestamp)).strftime('%Y%m%d')

    @staticmethod
    def __is_valid_response(json):
        return json is not None and json['status'] == 'ok'

    @staticmethod
    def __format_symbol(from_symbol, to_symbol):
        return ("%s%s" % (from_symbol, to_symbol)).lower()

    def run(self):
        while True:
            try:
                self.__pull_data_from_huobi(self.parent_dir, self.time_window)
            except Exception as e:
                self.logger.error("Unexpected Exception: " + str(e))
            time.sleep(Utils.get_seconds_from_time_window(self.time_window))


if __name__ == '__main__':
    HuobiClient("", "", '', '1min').run()

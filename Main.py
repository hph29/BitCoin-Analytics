import configparser
import multiprocessing
import sys

from Binance.BinanceClient import BinanceClient
from HitBTC.HitbtcClient import HitbtcClient
from Huobi.HuobiClient import HuobiClient


class Main:

    def __init__(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        self.root_dir = config['GENERAL']['StagingRootDir']
        self.interval = config['GENERAL']['PullingInterval']
        self.binance_public_key = config['BINANCE']['PublicKey']
        self.binance_private_key = config['BINANCE']['PrivateKey']
        self.huobi_public_key = config['HUOBI']['PublicKey']
        self.huobi_private_key = config['HUOBI']['PrivateKey']
        self.hitbtc_public_key = config['HITBTC']['PublicKey']
        self.hitbtc_private_key = config['HITBTC']['PrivateKey']

    def start_binance_data_pulling(self):
        BinanceClient(self.binance_private_key, self.binance_public_key, self.root_dir, self.interval).run()

    def start_huobi_data_pulling(self):
        HuobiClient(self.huobi_private_key, self.huobi_public_key, self.root_dir, self.interval).run()

    def start_hitbtc_data_pulling(self):
        HitbtcClient(self.hitbtc_private_key, self.hitbtc_public_key, self.root_dir, self.interval).run()

    def run(self):
        processes = list()
        processes.append(multiprocessing.Process(target=self.start_binance_data_pulling))
        processes.append(multiprocessing.Process(target=self.start_huobi_data_pulling))
        processes.append(multiprocessing.Process(target=self.start_hitbtc_data_pulling))
        for p in processes:
            p.start()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        Main('login.ini').run()
    else:
        Main(sys.argv[1]).run()
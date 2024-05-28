import kuaiqi_account as my_account
# 创建API实例,传入自己的快期账户
account_name = my_account.kuaiqi_account_name
account_password = my_account.kuaiqi_account_password


# Tauros_strategy1.0/data_layer/fetch_marketdata.py


import pandas as pd
from tqsdk import TqApi, TqAuth
from datetime import datetime
import os
import time

class DataManager:
    """
    DataManager类用于管理市场数据的获取和存储。
    """
    def __init__(self, symbols, auth):
        """
        初始化DataManager实例。

        参数:
        - symbols: 要订阅的合约列表
        - auth: 快期账户认证信息
        """
        # 初始化TqApi实例，并设置调试信息输出到debug.log文件
        self.api = TqApi(debug="D:/github/Tauros_strategy1.0/temp/debug.log", auth=TqAuth(account_name, account_password))
        self.symbols = symbols
        # 定义需要记录的字段
        self.fields = [
            'datetime', 'ask_price1', 'ask_volume1', 'bid_price1', 'bid_volume1',
            'ask_price2', 'ask_volume2', 'bid_price2', 'bid_volume2',
            'ask_price3', 'ask_volume3', 'bid_price3', 'bid_volume3',
            'ask_price4', 'ask_volume4', 'bid_price4', 'bid_volume4',
            'ask_price5', 'ask_volume5', 'bid_price5', 'bid_volume5',
            'last_price', 'highest', 'lowest', 'open', 'close', 'average', 'volume',
            'amount', 'open_interest', 'settlement', 'upper_limit', 'lower_limit',
            'pre_open_interest', 'pre_settlement', 'pre_close', 'price_tick', 'price_decs',
            'volume_multiple', 'max_limit_order_volume', 'max_market_order_volume',
            'min_limit_order_volume', 'min_market_order_volume'
        ]

    def get_hdf5_filename(self, symbol, data_type):
        """
        获取HDF5文件名。

        参数:
        - symbol: 合约代码
        - data_type: 数据类型（tick或kline）

        返回:
        - 生成的HDF5文件名
        """
        current_date = datetime.now().strftime("%Y-%m-%d")
        return f"{symbol}_{data_type}_{current_date}.h5"

    def fetch_tick_data(self, symbol):
        """
        获取指定合约的tick数据并存储到HDF5文件中。

        参数:
        - symbol: 合约代码
        """
        quote = self.api.get_quote(symbol)
        new_data = {field: quote[field] for field in self.fields if field in quote}
        df = pd.DataFrame([new_data])
        hdf5_file = self.get_hdf5_filename(symbol, "tick")
        with pd.HDFStore(hdf5_file, mode='a') as store:
            store.append('tick_data', df)

    def fetch_kline_data(self, symbol):
        """
        获取指定合约的10秒K线数据并存储到HDF5文件中。

        参数:
        - symbol: 合约代码
        """
        klines = self.api.get_kline_serial(symbol, duration_seconds=10)
        df = pd.DataFrame(klines)
        hdf5_file = self.get_hdf5_filename(symbol, "kline")
        with pd.HDFStore(hdf5_file, mode='a') as store:
            store.append('kline_data', df)

    def fetch_data_for_all_symbols(self):
        """
        获取所有合约的tick数据和10秒K线数据。
        """
        for symbol in self.symbols:
            self.fetch_tick_data(symbol)
            self.fetch_kline_data(symbol)

    def close(self):
        """
        关闭API连接。
        """
        self.api.close()

if __name__ == "__main__":
    # 快期账户认证信息
    auth = {"username": "your_username", "password": "your_password"}
    # 要订阅的合约列表
    symbols = ['SHFE.zn2406', 'DCE.m2101']  # 添加你需要的品种
    # 初始化DataManager实例
    data_manager = DataManager(symbols, auth)

    try:
        while True:
            # 等待数据更新
            data_manager.api.wait_update()
            # 获取所有合约的数据
            data_manager.fetch_data_for_all_symbols()
            time.sleep(1)
    finally:
        # 确保关闭API连接
        data_manager.close()
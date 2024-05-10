import pandas as pd
from datetime import datetime
from tqsdk import TqApi, TqAuth

import kuaiqi_account as my_account
# 创建API实例,传入自己的快期账户
account_name = my_account.kuaiqi_account_name
account_password = my_account.kuaiqi_account_password

# 初始化 API，并设置调试信息输出到 debug.log 文件
api = TqApi(debug="D:/github/Tauros_strategy1.0/temp/debug.log", auth=TqAuth(account_name, account_password))

# 订阅合约获取实时行情
contract_id = "SHFE.zn2406"
quote = api.get_quote(contract_id)

# 通过观察，选择您希望记录的字段
fields = [
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

# 初始化 DataFrame
df = pd.DataFrame(columns=fields)

# 首次写入 DataFrame，包括头部
csv_file = contract_id + '_data.csv'
df.to_csv(csv_file, mode='w', header=True, index=False)


try:
    while True:
        api.wait_update()
        # 检查 quote 是否有更新
        if api.is_changing(quote):
            # 从 quote 中获取所有字段的当前值
            new_data = {field: quote[field] for field in fields if field in quote}
            # 使用 quote 提供的 datetime，转换为合适的格式
            #new_data['datetime'] = pd.to_datetime(new_data['datetime']).strftime("%Y-%m-%d %H:%M:%S.%f")
            
            # 将新数据追加到 DataFrame
            df = df.append(new_data, ignore_index=True)
            
            # 追加数据到 CSV 文件，不包括头部信息（header=False）
            df.to_csv(csv_file, mode='a', header=False, index=False)
            df.drop(df.index, inplace=True)  # 清空 DataFrame 以节省内存

finally:
    api.close()  # 确保正确关闭 API 连接
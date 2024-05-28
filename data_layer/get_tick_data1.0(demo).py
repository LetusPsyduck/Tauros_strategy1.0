import pandas as pd
from datetime import datetime
from tqsdk import TqApi, TqAuth

import kuaiqi_account as my_account
# 创建API实例,传入自己的快期账户
account_name = my_account.kuaiqi_account_name
account_password = my_account.kuaiqi_account_password

# 初始化 API，并设置调试信息输出到 debug.log 文件
api = TqApi(debug="D:/github/Tauros_strategy1.0/temp/debug.log", auth=TqAuth(account_name, account_password))

# 订阅合约
quote = api.get_quote("SHFE.ZN2406")

# 准备数据框架来存储行情数据
df = pd.DataFrame(columns=['datetime', 'last_price', 'ask_price1', 'bid_price1'])

try:
    while True:
        api.wait_update()
        # 检查合约的价格或盘口是否有变化
        if api.is_changing(quote, "last_price") or api.is_changing(quote, ["ask_price1", "bid_price1"]):
            # 记录新的行情数据
            new_data = {
                'datetime': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'last_price': quote["last_price"],
                'ask_price1': quote["ask_price1"],
                'bid_price1': quote["bid_price1"]
            }
            df = df.append(new_data, ignore_index=True)
            # 打印新数据
            print(new_data)
            # 每次有新数据时保存更新
            df.to_csv('data.csv', index=False)
except KeyboardInterrupt:
    print("程序中断")
finally:
    api.close()  # 确保正确关闭 API 连接
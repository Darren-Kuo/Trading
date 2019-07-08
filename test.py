import pandas as pd
from pymongo import MongoClient
from datetime import timedelta
from matplotlib import pyplot as plt
from Visual import Visual
from datetime import datetime as dt


# from AlgoBank import AlgoBank

# algobank = AlgoBank()
# print(algobank.LOGIN(27402581, 'darrentest1', '123456'))
# date_from = dt.strptime('2018-01-01', '%Y-%m-%d')
# date_to = dt.strptime('2018-06-10', '%Y-%m-%d')
# data = algobank.Get_Market_his('CFH', 'USDCNH', date_from, date_to).reset_index(level = 'Time')
# df = data[['Time', 'Open']]
# df = df.rename({'Time':'order_time', 'price':'Open'})
# df = df.sample(n = 300).sort_values(by = ['Time'], ascending = [True])
# df.to_csv('test.csv')

# url = 'mongodb://admin:26552582@192.168.0.130:38128'

# # data_client = MongoClient(url)['DDPG_his_min']['BF_BTC_tick']
# data_client = MongoClient(url)['CFH_his_min']['CFH_EURUSD']
# data_client = MongoClient(url)['TWSE_his']['TWSE_1258']

# date_from = dt.strptime('2018-06-01', '%Y-%m-%d')
# date_to = dt.strptime('2018-06-03', '%Y-%m-%d')
# # data = pd.DataFrame(list(data_client.find({}, {'_id': False})))
# data = pd.DataFrame(list(data_client.find({'Time' : {'$gte' : date_from, '$lte':date_to}}, {'_id': False})))
# print(data.tail(5))


# data = data.set_index('Time')
# ohlc = data.Price.resample('1D').ohlc().dropna(axis=0, how='all')
# vol = data.Volume.resample('1D').sum().dropna(axis=0, how='all')
# data = pd.concat([ohlc, vol], axis=1)
# data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
# print(data.tail(20))

# tmp_dat = data
# visual = Visual()
# tmp_dat = tmp_dat.reset_index()
# tmp_dat['Date'] = tmp_dat['Time']
# visual.Plot_Candlestick(tmp_dat, 'TXF1', '0')
# visual.Show()

url = 'mongodb://admin:26552581@192.168.0.133:38128'
data_client = MongoClient(url)['DDPG_his_min']['TW_F_daily']
data = pd.DataFrame(list(data_client.find({}, {'_id': False})))
data.to_csv('TXF_daily.csv')



# col_list = MongoClient(url)['TWSE_2015'].collection_names()
# col_len = len(col_list)
# aa = 1
# for c in col_list:
# 	print(c, '.......', aa, '/', col_len)
# 	client_from = MongoClient(url)['TWSE_2015'][c]
# 	dat = client_from.find()
# 	dat = list(dat)
# 	client_to = MongoClient(url)['TWSE_his'][c]
# 	client_to.insert_many(dat)
# 	aa += 1



## remove ###
# url = 'mongodb://admin:26552582@192.168.0.130:38128'


# col_list = MongoClient(url)['TWSE_his'].collection_names()
# col_len = len(col_list)
# aa = 1
# for c in col_list:
# 	print(c, '.......', aa, '/', col_len)
# 	client = MongoClient(url)['TWSE_his'][c]
# 	client.remove({'Time' : {'$gte' : dt.strptime('2015-01-01', '%Y-%m-%d'), '$lte':dt.strptime('2015-12-31', '%Y-%m-%d')}})
# 	aa += 1

# client = MongoClient(url)['DDPG_his_min']['BF_BTC_min']
# client.remove({'Time' : {'$gte' : dt.strptime('2018-04-09', '%Y-%m-%d')}})

# url = 'mongodb://admin:26552581@192.168.0.133:38128'

# client = MongoClient(url)['DDPG_his_min']['TXF1_min']
# client.remove({'Time' : {'$gt' : dt.strptime('2019-03-29 13:45:00', '%Y-%m-%d %H:%M:%S')}})


# ##### 刪除重複資料 ##########

# url = 'mongodb://admin:26552582@192.168.0.130:38128'
# date_from = dt.strptime('2009-06-05', '%Y-%m-%d')
# date_to = dt.strptime('2009-06-06', '%Y-%m-%d')


# col_list = MongoClient(url)['TWFE_his_min'].collection_names()


# for c in col_list:
# 	# print(c)
# 	data_client = MongoClient(url)['TWFE_his_min'][c]

# 	data = pd.DataFrame(list(data_client.find({'Time' : {'$gte' : date_from, '$lte':date_to}})))


# 	if len(data) != 0:
# 		if len(data) != data['Time'].nunique():
# 			print(c)
		# delete_list = data['_id'][data.duplicated('Time')]
		# print(delete_list)
		# for d in delete_list:
		# 	# print(type(d))
		# 	data_client.remove({'_id' : d })


### delete column name ###
# url = 'mongodb://admin:26552581@192.168.0.132:38128'

# col_list = MongoClient(url)['TWSE_his'].collection_names()
# col_len = len(col_list)
# aa = 1
# for c in col_list:
# 	print(c, '.......', aa, '/', col_len)
# 	client = MongoClient(url)['TWSE_his'][c]
# 	# client.update({}, {'$rename': {'Price': 'TradePrice', 'ID': 'CommodityNo', 'Volume':'TradeVolume'}}, multi=True)
# 	client.update({}, {'$rename': {'ID': 'CommodityNo', 'Price': 'TradePrice', 'Volume':'TradeVolume', 
# 									'Ask_Price_1':'AskPrice1', 'Ask_Volume_1':'AskVolume1',
# 									'Ask_Price_2':'AskPrice2', 'Ask_Volume_2':'AskVolume2',
# 									'Ask_Price_3':'AskPrice3', 'Ask_Volume_3':'AskVolume3',
# 									'Ask_Price_4':'AskPrice4', 'Ask_Volume_4':'AskVolume4',
# 									'Ask_Price_5':'AskPrice5', 'Ask_Volume_5':'AskVolume5',
# 									'Bid_Price_1':'BidPrice1', 'Bid_Volume_1':'BidVolume1',
# 									'Bid_Price_2':'BidPrice2', 'Bid_Volume_2':'BidVolume2',
# 									'Bid_Price_3':'BidPrice3', 'Bid_Volume_3':'BidVolume3',
# 									'Bid_Price_4':'BidPrice4', 'Bid_Volume_4':'BidVolume4',
# 									'Bid_Price_5':'BidPrice5', 'Bid_Volume_5':'BidVolume5',
# 									}}, multi=True)
# 	aa += 1


### delete column name ###
# url = 'mongodb://admin:26552581@192.168.0.132:38128'

# col_list = MongoClient(url)['TWSE_min_2016'].collection_names()
# col_len = len(col_list)
# aa = 1
# for c in col_list:
# 	print(c, '.......', aa, '/', col_len)
# 	client = MongoClient(url)['TWSE_min_2016'][c]
# 	client.update({}, {'$unset': {'ID': ""}}, multi=True)
# 	aa += 1


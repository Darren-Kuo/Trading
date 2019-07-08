import json
import time
import random
import numpy as np
import talib
import threading
import bitfinexpy
import pandas as pd
import signal as sg
from MarketData import MarketData
from AlgoBank import AlgoBank
from datetime import datetime as dt
from TrendSignal import TrendSignal
from datetime import timedelta

sg.signal(sg.SIGINT, sg.SIG_DFL)

### instance para ###
EXCHANGE = 'BTF'
MERCHANDISE = 'ETHUSD'

DETAIL_FILE = 'detail.csv'
POSITION_FILE = 'position.csv'

O_TYPE = 'MKT'
E_TYPE = 'margin'
FEES = 0.002
TRAILING_POINT = 2
CONSTRUCT_AMOUNT = 80
CONSTRUCT_COUNTS = 10
MAX_POSITION = 5

STOPLOSS = 0.03
STOPLOSS_POINT = 0.005
STOPPROFIT = 0.08
STOPPROFIT_POINT = 0.02

TRAIL_FLAG_1 = '1'
TRAIL_FLAG_2 = '2'

def Logs(msg):
	print('[{}] {}'.format(dt.now().strftime('%F %T'), msg))

class ETH_algo15(object):
	def __init__(self, algobank, btf):
		self.long_pos = 0
		self.short_pos = 0

		self.algobank = algobank
		self.btf = btf
		self.ts = TrendSignal()
		self.marketdata = MarketData()

		self.position = pd.DataFrame()
		self.detail = pd.DataFrame()

		self.LOCK_1 = threading.Lock()
		self.LOCK_2 = threading.Lock()

		self.side = 'none'
		self.ow_threshold = 0
		self.avg_price = 0
		self.stopprofit_th = STOPPROFIT
		self.stop_percent = STOPLOSS
		self.stop_price = 0
		self.canTakeProfit = False
		self.canOverweight = False
		self.isMonitor = False
		self.canOrder = True

		self.trailFlag = TRAIL_FLAG_1


	def update_signal(self, df):
		self.ts.first_update(df)
		self.first_sig = self.ts.first_sig
		self.second_sig = self.ts.second_sig
		
		# print(self.cur_time, self.first_sig, self.second_sig)


	def btf_open(self, symbol, price, volume, buy_sell, order_type = 'MKT', ex_type = 'exchange'):
		try:
			if order_type == 'LMT':
				_type = 'exchange limit' if ex_type == 'exchange' else 'limit'
				order_ref = self.btf.new_order(symbol, str(volume), str(price), buy_sell, _type)

			elif order_type == 'MKT':
				_type = 'exchange market' if ex_type == 'exchange' else 'market'
				order_ref = self.btf.new_order(symbol, str(volume), str(price), buy_sell, _type)

			return order_ref['id']

		except BaseException:
			Logs('[System] Open order error, wait 3 sec')
			time.sleep(3)
			_id = self.btf_open(symbol, price, volume, buy_sell, order_type, ex_type)
			
			return _id


	def btf_close(self, symbol, price, volume, buy_sell, order_type = 'MKT', ex_type = 'exchange'):
		try:
			if order_type == 'LMT':
				_type = 'exchange limit' if ex_type == 'exchange' else 'limit'
				order_ref = self.btf.new_order(symbol, str(volume), str(price), buy_sell, _type)

			elif order_type == 'MKT':
				_type = 'exchange market' if ex_type == 'exchange' else 'market'
				order_ref = self.btf.new_order(symbol, str(volume), str(price), buy_sell, _type)

			return order_ref['id']

		except BaseException:
			Logs('[System] Close order error, wait 3 sec')
			time.sleep(3)
			_id = self.btf_close(symbol, price, volume, buy_sell, order_type, ex_type)

			return _id


	def btf_order_status(self, order_id):
		try:
			order_ref = self.btf.order_status(order_id)

			return order_ref
		except:
			Logs('[System] Get order status, wait 3 sec')
			time.sleep(3)
			order_ref = self.btf_order_status(order_id)

			return order_ref


	def btf_active(self):
		try:
			result = self.btf.active_positions()
			active_info = []
			if len(result) != 0:
				for r in result:
					if r['symbol'] == MERCHANDISE.lower():
						active_info.append(r)

				if len(active_info) != 0:
					pl = float(active_info[0]['pl'])
					price = float(active_info[0]['base'])
					amount = abs(float(active_info[0]['amount']))

					rate_of_return = pl / (price * amount)
			else:
				rate_of_return = 0

			return rate_of_return
		except:
			Logs('[System] Get active info error, wait 3 sec')
			time.sleep(3)
			rate_of_return = self.btf_active()

			return rate_of_return


	def btf_get_avg_price(self, order_id):
		avg_price = 0
		o_amount = 'o_amount'
		e_amount = 'e_amount'

		while o_amount != e_amount:
			order_ref = self.btf_order_status(order_id)
			avg_price = order_ref['avg_execution_price']
			o_amount = order_ref['original_amount']
			e_amount = order_ref['executed_amount']
			time.sleep(2)

		return avg_price


	def ref_to_df(self, _id, side, price, amount):
		df = pd.DataFrame({'order_id':[_id],
						   'open_time':[dt.now().strftime('%F %T')],
						   'side':[side],
						   'open_price':[price],
						   'amount':[amount],
						   'closed':[False]
						  })
		df = df[['order_id', 'open_time', 'side', 'open_price', 'amount', 'closed']]

		self.position = self.position.append(df)
		self.position.reset_index(drop = True, inplace = True)
		self.position.to_csv(POSITION_FILE, index = False)


	def record_detail(self, _id, price, amount):
		_position = self.position.loc[self.position['order_id'] == _id]
		side = _position.iloc[-1]['side']
		open_time = _position.iloc[-1]['open_time']
		open_price = _position.iloc[-1]['open_price']
		amount = _position.iloc[-1]['amount']
		close_time = dt.now().strftime('%F %T')
		close_price = price
		commission = (open_price + close_price) * FEES * amount
		profit = ((close_price - open_price) * amount - commission) if side == 'buy' else ((open_price - close_price) * amount - commission)

		df = pd.DataFrame(
				{'open_time':[open_time], 
				'open_price':[open_price],
				'side': [side],
				'amount':[amount],
				'close_time':[close_time],
				'close_price':[close_price],
				'commission':[commission],
				'profit':[profit],
				'order_id':[_id]
				})
		df = df[['order_id', 'open_time', 'open_price', 'side', 'amount', 'close_time', 'close_price', 'commission', 'profit']]

		df.to_csv(DETAIL_FILE, mode = 'a', header = False, index = False)
		self.detail = self.detail.append(df)
		self.detail.reset_index(drop=True, inplace=True)

		self.position.loc[self.position['order_id'] == _id, 'closed'] = True


	def construct_order(self, side):
		_amount = CONSTRUCT_AMOUNT/CONSTRUCT_COUNTS
		_id = str(random.randint(1, 99999999))
		_sum_price = 0

		print('---------- Start open positions ----------')
		for c in range(CONSTRUCT_COUNTS):
			order_id = self.btf_open(MERCHANDISE, 1, _amount, side, O_TYPE, E_TYPE)
			time.sleep(3)

			p = self.btf_get_avg_price(order_id)
			Logs('{} a position @{}. ID is {}'.format(side, p, order_id))
			_sum_price += float(p)

		_avg_price = _sum_price / CONSTRUCT_COUNTS
		self.ref_to_df(_id, side, _avg_price, CONSTRUCT_AMOUNT)

		print('-------------------------------------------')
		Logs('{} {} ETH @{}'.format(side, CONSTRUCT_AMOUNT, _avg_price))

		if side == 'buy':
			self.long_pos += 1
		elif side == 'sell':
			self.short_pos += 1
		if self.long_pos == MAX_POSITION or self.short_pos == MAX_POSITION:
			self.canOverweight = False

		return _id, _avg_price
		

	def covering_order(self, _id, side):
		_amount = CONSTRUCT_AMOUNT/CONSTRUCT_COUNTS
		_side = 'buy' if side == 'sell' else 'sell'
		_sum_price = 0

		print('---------- Start close positions ----------')
		for c in range(CONSTRUCT_COUNTS):
			order_id = self.btf_open(MERCHANDISE, 1, _amount, _side, O_TYPE, E_TYPE)
			time.sleep(3)

			p = self.btf_get_avg_price(order_id)
			Logs('{} a position @{}. ID is {}'.format(_side, p, order_id))
			_sum_price += float(p)

		_avg_price = _sum_price / CONSTRUCT_COUNTS
		self.record_detail(_id, _avg_price, CONSTRUCT_AMOUNT)

		print('-------------------------------------------')
		Logs('{} {} ETH @{}'.format(_side, CONSTRUCT_AMOUNT, _avg_price))

		if side == 'buy':
			self.long_pos -= 1
		elif side == 'sell':
			self.short_pos -= 1


	def __MonitorThread(self):
		self.monitorThread = threading.Thread(name = 'monitorThread', target = self.monitoring)
		self.monitorThread.start()


	def monitoring(self):
		while self.isMonitor:
			cond1 = self.side == 'buy' and self.cur_price < self.stop_price
			cond2 = self.side == 'sell' and self.cur_price > self.stop_price

			if cond1 or cond2:
				print('------------------------------------')
				Logs('close all position by stop loss')
				Logs('AP:{}, SP:{}, P:{}'.format(self.avg_price, self.stop_price, self.cur_price))
				self.reset_trading_para()
				self.close_all_pos()
				self.side = 'none'
				print('------------------------------------')

			if self.canTakeProfit:
				cond1 = self.side == 'buy' and self.cur_price < self.stopprofit
				cond2 = self.side == 'sell' and self.cur_price > self.stopprofit

				if cond1 or cond2:
					print('------------------------------------')
					Logs('close all position by stop profit')
					Logs('AP:{}, TP:{}, P:{}'.format(self.avg_price, self.stopprofit, self.cur_price))
					self.reset_trading_para()
					self.close_all_pos()
					self.side = 'none'
					print('------------------------------------')
				else:
					cond1 = self.side == 'buy' and self.cur_price > self.avg_price * (1 + self.stopprofit_th)
					cond2 = self.side == 'sell' and self.cur_price < self.avg_price * (1 - self.stopprofit_th)

					if cond1:
						self.stopprofit = round(self.avg_price * (1 + (self.stopprofit_th - STOPPROFIT_POINT)), 2)
						self.stopprofit_th += STOPPROFIT_POINT
						Logs('Update Integral Take Profit, TP: {}'.format(self.stopprofit))
					elif cond2:
						self.stopprofit = round(self.avg_price * (1 - (self.stopprofit_th - STOPPROFIT_POINT)), 2)
						self.stopprofit_th += STOPPROFIT_POINT
						Logs('Update Integral Take Profit, TP: {}'.format(self.stopprofit))
			else:
				cond1 = self.side == 'buy' and self.cur_price > self.avg_price * (1 + STOPPROFIT)
				cond2 = self.side == 'sell' and self.cur_price < self.avg_price * (1 - STOPPROFIT)

				if cond1:
					self.stopprofit = round(self.avg_price * (1 + (STOPPROFIT - STOPPROFIT_POINT)), 2)
					self.canTakeProfit = True
					self.stopprofit_th = STOPPROFIT + STOPPROFIT_POINT
					Logs('Start Integral Take Profit, TP: {}'.format(self.stopprofit))
				elif cond2:
					self.stopprofit = round(self.avg_price * (1 - (STOPPROFIT - STOPPROFIT_POINT)), 2)
					self.canTakeProfit = True
					self.stopprofit_th = STOPPROFIT + STOPPROFIT_POINT
					Logs('Start Integral Take Profit, TP: {}'.format(self.stopprofit))


			time.sleep(5)

		Logs('Stop monitoring................')


	def __TrailThread(self, side):
		trailThread = threading.Thread(target = self.trailing, args = (side, self.trailFlag))
		trailThread.start()


	def trailing(self, side, flag):
		id_tag, price = self.construct_order(side)
		if side == 'buy':
			self.avg_price = round((self.avg_price * (self.long_pos - 1) + price) / self.long_pos, 2)
			self.stop_price = round(self.avg_price * (1 - self.stop_percent), 2)
			# self.stop_percent = self.stop_percent - STOPLOSS_POINT
		elif side == 'sell':
			self.avg_price = round((self.avg_price * (self.short_pos - 1) + price) / self.short_pos, 2)
			self.stop_price = round(self.avg_price * (1 + self.stop_percent), 2)
			# self.stop_percent = self.stop_percent - STOPLOSS_POINT

		if not self.isMonitor:
			self.isMonitor = True
			self.__MonitorThread()

		self.canOrder = True

		Logs('{} - Start trailing'.format(id_tag))
		setattr(self, id_tag, False)

		if side == 'buy':
			Logs('{} - Open long pos, OP: {}, SP: {}'.format(id_tag, round(price, 2), self.stop_price))
			lock = self.LOCK_1
			th = round(price + 2 * TRAILING_POINT, 5)

			do = True
			while do and flag == self.trailFlag:
				p_ = self.bid_price
				if p_ > th:
					lock.acquire()
					price = round(p_ - TRAILING_POINT, 5)

					Logs('{} - Update take profit at {}.'.format(id_tag, price))
					do = False
					setattr(self, id_tag, True)

					lock.release()

			th = round(price + 2 * TRAILING_POINT, 5)

			while getattr(self, id_tag) and flag == self.trailFlag:
				lock.acquire()
				do_ = True
				while do_ and flag == self.trailFlag:
					p_ = self.bid_price
					if p_ > th:
						price = round(p_ - TRAILING_POINT, 5)
						th = round(price + 2 * TRAILING_POINT, 5)
						Logs('{} - Update take profit at {}.'.format(id_tag, price))
					elif p_ <= price:
						self.covering_order(id_tag, side)
						setattr(self, id_tag, False)
						Logs('{} - Take profit at {}.'.format(id_tag, p_))
					do_ = False

				lock.release()


		elif side == 'sell':
			Logs('{} - Open short pos, OP: {}, SP: {}'.format(id_tag, round(price, 2), self.stop_price))
			lock = self.LOCK_2
			th = round(price - 2 * TRAILING_POINT, 5)

			do = True
			while do and flag == self.trailFlag:
				p_ = self.ask_price

				if p_ < th:
					lock.acquire()
					price = round(p_ + TRAILING_POINT, 5)
					Logs('{} - Update take profit at {}.'.format(id_tag, price))
					do = False
					setattr(self, id_tag, True)
					lock.release()


			th = round(price - 2 * TRAILING_POINT, 5)
        
			while getattr(self, id_tag) and flag == self.trailFlag:
				lock.acquire()

				do_ = True
				while do_ and flag == self.trailFlag:
					p_ = self.ask_price
					if p_ < th:
						price = round(p_ + TRAILING_POINT, 5)
						th = round(price - 2 * TRAILING_POINT, 5)
						Logs('{} - Update take profit at {}.'.format(id_tag, price))
					elif p_ >= price:
						self.covering_order(id_tag, side)
						setattr(self, id_tag, False)
						Logs('{} - Take profit at {}.'.format(id_tag, p_))
					do_ = False

				lock.release()

		delattr(self, id_tag)

		Logs('{} - Stop trailing'.format(id_tag))


	def condition_to_open(self):
		cond1 = self.first_sig == 'B' and self.second_sig == 'B' and self.long_pos == 0 and self.canOrder == True
		cond2 = self.first_sig == 'S' and self.second_sig == 'S' and self.short_pos == 0 and self.canOrder == True

		if cond1:
			self.ow_threshold = self.ask_price + TRAILING_POINT
			self.canOverweight = True
			self.side = 'buy'
			self.__TrailThread(side = self.side)
			self.canOrder = False

		elif cond2:
			self.ow_threshold = self.bid_price - TRAILING_POINT
			self.canOverweight = True
			self.side = 'sell'
			self.__TrailThread(side = self.side)
			self.canOrder = False


	def condition_to_close(self):
		cond1 = self.long_pos > 0 and (self.first_sig == 'S')
		cond2 = self.short_pos > 0 and (self.first_sig == 'B')

		if cond1:
			Logs('close by signal change')
			self.reset_trading_para()
			self.close_all_pos()
			self.side = 'none'
		elif cond2:
			Logs('close by signal change')
			self.reset_trading_para()
			self.close_all_pos()
			self.side = 'none'


	def condition_to_overweight(self):
		cond3 = self.side == 'buy' and self.canOverweight and self.bid_price >= self.ow_threshold and self.canOrder == True
		cond4 = self.side == 'sell' and self.canOverweight and self.ask_price <= self.ow_threshold and self.canOrder == True

		if cond3 or cond4:
			if cond3:
				self.ow_threshold += TRAILING_POINT
				Logs('Overweight a long position, have {} positions'.format(self.long_pos))
				self.canOrder = False
				self.__TrailThread(side = self.side)
			elif cond4:
				self.ow_threshold -= TRAILING_POINT
				Logs('Overweight a short position, have {} positions'.format(self.short_pos))
				self.canOrder = False
				self.__TrailThread(side = self.side)

			Logs('Overweight threshold update to {}'.format(self.ow_threshold))


	def close_all_pos(self):
		side = self.side

		if side == 'buy':
			_positions = self.position.loc[(self.position['side'] == 'buy') & (self.position['closed'] == False)]
			p = self.bid_price

		elif side == 'sell':
			_positions = self.position.loc[(self.position['side'] == 'sell') & (self.position['closed'] == False)]
			p = self.ask_price

		pos_list = _positions.order_id.tolist()
		for l in pos_list:
			self.covering_order(l, side)

		Logs('Close all positions.......')


	def reset_trading_para(self):
		Logs('Reset trade status')
		
		self.isMonitor = False
		self.canTakeProfit = False
		self.canOverweight = False
		self.avg_price = 0
		self.stop_price = 0
		self.stop_percent = STOPLOSS

		if self.trailFlag == TRAIL_FLAG_1:
			self.trailFlag = TRAIL_FLAG_2
		elif self.trailFlag == TRAIL_FLAG_2:
			self.trailFlag = TRAIL_FLAG_1


	def update_data(self):
		self.cur_itv = pd.to_datetime(dt.strftime(self.cur_time, '%F %H'))
		self.past_itv = self.marketdata.hour.tail(1).index[0]
		self.marketdata.update('hour', self.cur_price, self.cur_volume, self.past_itv, self.cur_itv)


	def run(self):
		minute = self.algobank.Get_Market_his_tail(EXCHANGE, MERCHANDISE, 43200, 'min')
		hour = self.marketdata.to_period(df = minute, interval = 1, unit = 'H')
		self.marketdata.set_new_data('hour', hour)


		for i in range(81, len(self.marketdata.hour)):
			self.window_dat = self.marketdata.hour[(i-81):i]
			self.cur_time = self.marketdata.hour.index[i]

			self.update_signal(self.window_dat)

		self.algobank.AddTarget(EXCHANGE, MERCHANDISE)

		while True:
			tick = self.algobank.GetInstancePrice(EXCHANGE, MERCHANDISE)
			if tick != '':
				_tick = json.loads(tick)
				self.cur_time = dt.fromtimestamp(float(_tick['timestamp']))
				self.ask_price = float(_tick['ask'])
				self.bid_price = float(_tick['bid'])
				self.cur_price = float(_tick['last_price'])
				self.cur_volume = float(_tick['volume'])

				self.update_data()
				self.update_signal(self.marketdata.hour.tail(81))
				self.condition_to_close()
				self.condition_to_overweight()
				self.condition_to_open()

if __name__ == '__main__':
	algobank = AlgoBank()
	
	result = algobank.LOGIN(00000000, 'aaaaa', '123456')

	if result['result']:
		btf = bitfinexpy.API(environment = 'live',
							 key = 'xxxxxxxxxxxxxxx',
							 secret_key = 'xxxxxxxxxxxxxxx')
		algo = ETH_algo15(algobank, btf)
		algo.run()
	else:
		Logs('Login failed')

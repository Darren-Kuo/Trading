import talib
import numpy as np

class TrendSignal(object):
    def __init__(self):
        self.first_sig = 'N'
        self.second_sig = 'N'
        self.daily_sig = 'N'


    def daily_update(self, df):
        df_data = df.copy().fillna(0)
        df_data['ma'] = df_data.Close.rolling(10).mean()

        _ma = df_data.ma.iloc[-1]
        _open = df_data.Open.iloc[-1]
        _close = df_data.Close.iloc[-1]

        if _open < _ma < _close:
            self.daily_sig = 'B'
        elif _open > _ma < _close:
            self.daily_sig = 'S'


    def first_update(self, df):
        df_data = df.copy().fillna(0)
        df_data['ma_short'] = df_data.Close.rolling(20).mean()
        df_data['ma_short_h'] = df_data.High.rolling(20).mean()
        df_data['ma_short_l'] = df_data.Low.rolling(20).mean()
        df_data['ma_long'] = df_data.Close.rolling(80).mean()
        df_data['ma_long_h'] = df_data.High.rolling(80).mean()
        df_data['ma_long_l'] = df_data.Low.rolling(80).mean()

        _ma_s = df_data.ma_short.iloc[-1]
        _ma_l = df_data.ma_long.iloc[-1]
        _ma_l_h = df_data.ma_long_h.iloc[-1]
        _ma_l_l = df_data.ma_long_l.iloc[-1]
        _ma_s_h = df_data.ma_short_h.iloc[-1]
        _ma_s_l = df_data.ma_short_l.iloc[-1]
        _open = df_data.Close.iloc[-2]
        _close = df_data.Close.iloc[-1]
        

        if _ma_s > _ma_l and _ma_l_h < _close:
            self.first_sig = 'B'
        elif _ma_s < _ma_l and _ma_l_l > _close:
            self.first_sig = 'S'
        else:
            self.first_sig = 'N'


        if _open < _ma_s_h < _close:
            self.second_sig = 'B'
        elif _open > _ma_s_l > _close:
            self.second_sig = 'S'
        else:
            self.second_sig = 'N'

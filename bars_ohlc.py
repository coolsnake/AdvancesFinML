import pandas as pd
import numpy as np
import os
import datetime

## Frequency examples
# 1M  == 1 month
# 1H == 1 hour
# 1Min == 1 minute
# 1S = 1 second
# 1ms = 1 millisecond
# 1U = 1 microsecond
# 1N = 1 nanosecond

# this module is used to generate volume bar from candlestick.

class BarSeries(object):

    def __init__(self, df, timecolumn='datetime'):
        self.df = df
        self.timecolumn = timecolumn

class TickBarSeries(BarSeries):

    def __init__(self, df, timecolumn='datetime', volume_column='volume'):
        self.volume_column = volume_column
        super(TickBarSeries, self).__init__(df, timecolumn)

    def process_ohlc(self, open_name, high_name, low_name, close_name, frequency):

        data = []

        for i in range(frequency, len(self.df), frequency):
            sample = self.df.iloc[i - frequency:i]

            volume = sample[self.volume_column].values.sum()
            open = sample[open_name].values.tolist()[0]
            high = sample[high_name].values.max()
            low = sample[low_name].values.min()
            close = sample[close_name].values.tolist()[-1]
            time = sample.index.values[-1]

            data.append({
                self.timecolumn: time,
                'open': open,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume
            })

        data = pd.DataFrame(data).set_index(self.timecolumn)
        return data

    def process_ticks(self, open_column = 'open', high_column = 'high', low_column = 'low', close_column = 'close',  frequency='15Min'):
        ohlc_df = self.process_ohlc(open_column,high_column,low_column,close_column, frequency)
        return ohlc_df

class VolumeBarSeries(BarSeries):

    def __init__(self, df, timecolumn='datetime', volume_column='volume'):
        self.volume_column = volume_column
        super(VolumeBarSeries, self).__init__(df, timecolumn)

    def process_ohlc(self, open_name, high_name, low_name, close_name, frequency):
        data = []
        op_buf = []
        hi_buf = []
        lo_buf = []
        cl_buf = []

        start_index = 0.
        volume_buf = 0.

        for i in range(len(self.df[open_name])):

            op_p_i = self.df[open_name].iloc[i]
            hi_p_i = self.df[high_name].iloc[i]
            lo_p_i = self.df[low_name].iloc[i]
            cl_p_i = self.df[close_name].iloc[i]
            v_i = self.df[self.volume_column].iloc[i]
            d_i = self.df.index.values[i]

            op_buf.append(op_p_i)
            hi_buf.append(hi_p_i)
            lo_buf.append(lo_p_i)
            cl_buf.append(cl_p_i)
            volume_buf += v_i

            if volume_buf >= frequency:

                open = op_buf[0]
                high = np.max(hi_buf)
                low = np.min(lo_buf)
                close = cl_buf[-1]
                #print(d_i)
                data.append({
                    self.timecolumn: d_i,
                    'open': open,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume_buf
                })

                op_buf, hi_buf, lo_buf, cl_buf, volume_buf = [], [], [], [], 0.

        data = pd.DataFrame(data).set_index(self.timecolumn)
        return data

    def process_ticks(self, open_column = 'open', high_column = 'high', low_column = 'low', close_column = 'close', volume_column='volume', frequency='15Min'):
        ohlc_df = self.process_ohlc(open_column, high_column,low_column,close_column, frequency)
        return ohlc_df

class DollarBarSeries(BarSeries):

    def __init__(self, df, timecolumn = 'datetime', volume_column='volume'):
        self.volume_column = volume_column
        super(DollarBarSeries, self).__init__(df, timecolumn)

    def process_ohlc(self, open_name, high_name, low_name, close_name, frequency):

        data = []
        op_buf = []
        hi_buf = []
        lo_buf = []
        cl_buf = []
        vbuf = []
        time_diff_buf = []
        start_index = 0.
        dollar_buf = 0.

        for i in range(len(self.df[open_name])):
            op_p_i = self.df[open_name].iloc[i]
            hi_p_i = self.df[high_name].iloc[i]
            lo_p_i = self.df[low_name].iloc[i]
            cl_p_i = self.df[close_name].iloc[i]
            
            v_i = self.df[self.volume_column].iloc[i]
            d_i = self.df.index.values[i]

            dv_i = cl_p_i * v_i
            op_buf.append(op_p_i)
            hi_buf.append(hi_p_i)
            lo_buf.append(lo_p_i)
            cl_buf.append(cl_p_i)
            
            vbuf.append(v_i)
            dollar_buf += dv_i

            if dollar_buf >= frequency:

                open = op_buf[0]
                high = np.max(hi_buf)
                low = np.min(lo_buf)
                close = cl_buf[-1]
                volume = np.sum(vbuf)

                data.append({
                    self.timecolumn: d_i,
                    'open': open,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume,
                    'dollar': dollar_buf
                })

                op_buf, hi_buf, lo_buf, cl_buf, vbuf, dollar_buf = [], [], [], [], [], 0

        data = pd.DataFrame(data).set_index(self.timecolumn)
        return data

    def process_ticks(self, open_column = 'open', high_column = 'high', low_column = 'low', close_column = 'close', volume_column='volume', frequency=10000):
        ohlc_df = self.process_ohlc(open_column, high_column,low_column,close_column, frequency)
        return ohlc_df
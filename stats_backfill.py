import pandas as pd
pd.core.common.is_list_like = pd.api.types.is_list_like
import pandas_datareader.data as web
import numpy as np
from datetime import datetime, timedelta
import logging
from stockstats import StockDataFrame
import sys

sys.path.insert(0, '/home/jerseyfinance2018/Notebooks/API')
from DBUtil import DBUtil

logger = logging.getLogger('stats_backfill')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('/home/jerseyfinance2018/scripts/stats_backfill.log')
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

tickers = DBUtil.getAllTickers(False)
for ticker in tickers:
    logger.info('getting daily price for ' + ticker)
    try:
        df = pd.DataFrame(DBUtil.getDailyPrices(ticker, 20180601, 30180729))
        stock = StockDataFrame.retype(df)
        stock.get('close_5_ema')
        stock.get('close_10_ema')
        stock.get('close_20_ema')
        stock.get('close_30_ema')
        stock.get('rsi_14')
        stock.get('macd')

        DBUtil.batchInsertStats(df, ticker, 'day', 'macd', 'macds', 'macdh', 'close_5_ema', 'close_10_ema', 'close_20_ema', 'close_30_ema', 'rsi_14', 'volume_50_sma')
        #DBUtil.batchUpdateStats(ticker, df.to_dict('records'), 'volume_50_sma', 'volume_50_sma')
    except Exception as e:
            logger.error(e)
            print(e)
            continue

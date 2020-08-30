## First we import the required packages:
import numpy as np, pandas as pd
from datetime import timedelta
import time
from feature_selector import FeatureSelector
import datetime
import pickle
import progressbar
from sklearn.impute import SimpleImputer
import dateutil.relativedelta
import multiprocessing as mp
import matplotlib.pyplot as plt

'''
## We then load the required yield and financial ratios database:
ratio= pd.read_csv('ratios_1990_2019.csv')
ratio = ratio.convert_objects(convert_numeric=True)
yields=pd.read_csv('yield_1962_2019.csv')

ticker=yields.tic ## Save for later since convert_numeric will break the strings
yields = yields.convert_objects(convert_numeric=True)
yields.tic=ticker

# We wanna use the datetime format to facilitate the next steps so let's change it right now:
ratio.public_date=pd.to_datetime(ratio.public_date)
yields.datadate=pd.to_datetime(yields.datadate)

#remove non-common shares from database
yields = yields[yields.tpci == 0]

## We will want to use only the constituents from the S&P1500
## The selection will be in the Data Cleaning section below
SP1500constituents=pd.read_csv('SP1500constituents.csv')
SP1500constituents['from_date']=pd.to_datetime(SP1500constituents['from_date'])
SP1500constituents['thru_date']=pd.to_datetime(SP1500constituents['thru_date'])


## To alleviate the memory needed we can keep only the data from January 1995:
d = datetime.datetime.strptime("1995-01-01", "%Y-%m-%d")
d2= datetime.datetime.strptime("2019-08-31", "%Y-%m-%d")
ratio= ratio[ratio["public_date"].isin(pd.date_range(d, d2))]
yields= yields[yields["datadate"].isin(pd.date_range(d, d2))]

# Let's reset the index:
ratio=ratio.reset_index(drop=True)
yields=yields.reset_index(drop=True)
'''


dfm = pd.read_csv('data/monthly/agg/monthly.csv').set_index(['date', 'symbol'])
dfm['trt1m'] = dfm.groupby(level=1).diff(-1).close * -1 / dfm.close

dfm_ = dfm.reset_index().set_index('date')
dfm_spy = dfm.iloc[dfm.index.get_level_values('symbol') == 'SPY'].groupby(level=1).diff(-1).close * -1 / dfm.iloc[dfm.index.get_level_values('symbol') == 'SPY'].close
dfm_spy = dfm_spy.reset_index().set_index('date').dropna()
dfm_['sprtrn'] = dfm_spy.close
dfm_ = dfm_.dropna()

df_mrq = pd.read_csv('data/SHARADAR_SF1_montly_combined_universe_MRQ.csv').rename(columns={'ticker': 'symbol', 'calendardate': 'date'}).set_index(['date', 'symbol'])
df_mrq.isna().sum()

df_mrq = df_mrq.loc[:, df_mrq.isnull().mean() < .5]

imp=SimpleImputer(missing_values=np.nan, strategy="mean")
impute_columns = df_mrq.columns[df_mrq.isna().any()].tolist()
for i in impute_columns: df_mrq[i] = imp.fit_transform(df_mrq[[i]])

dfm__ = dfm_.reset_index().set_index(['date', 'symbol'])
dfm__['win'] = (dfm__['trt1m'] > dfm__['sprtrn']).astype(np.int64)
dfm__['rtoversp'] = dfm__['trt1m'] - dfm__['sprtrn']
dfm__ = dfm__.dropna()
dfm__.isna().sum()

df_mrq['win'] = dfm__.win
df_mrq['trt1m'] = dfm__.trt1m
df_mrq['sprtrn'] = dfm__.sprtrn
df_mrq['rtoversp'] = dfm__.rtoversp
df_mrq = df_mrq.dropna()


train = df_mrq.drop(columns = ['dimension', 'win', 'rtoversp'])
train_labels = df_mrq['win']

fs = FeatureSelector(data = train, labels = train_labels)
fs.identify_collinear(correlation_threshold=0.975)

#fs.plot_collinear(plot_all=True)

#fs.identify_zero_importance(task = 'regression', eval_metric = 'auc', n_iterations = 10, early_stopping = True)

#fs.identify_low_importance(cumulative_importance = 0.99)

all_to_remove = fs.check_removal()
print(all_to_remove)

df_mrq_pruned = df_mrq.drop(columns = all_to_remove)


# df_mrq_pruned.to_csv('data/SHARADAR_SF1_montly_combined_universe_MRQ.labelled.csv')









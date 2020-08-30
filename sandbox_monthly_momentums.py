import pandas as pd


df = pd.read_csv('data/monthly/agg/monthly.csv').reset_index(['date', 'symbol'])

for i in range(1, 11):
    print('rtr{i}m'.format(i=i))
    df['rtr{i}m'.format(i=i)] = df.groupby(level=1).diff(i).close / df.groupby(level=1).shift(i).close

df_momentums = df.dropna()
df_momentums = df_momentums[['rtr{i}m'.format(i=i) for i in range(1, 11)]].round(3)


df_momentums.to_csv('monthly_momentums.csv')


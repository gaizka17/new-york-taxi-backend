from pandas import read_csv
from pandas import datetime
import pandas as pd
from pandas import DataFrame
from statsmodels.tsa.arima_model import ARIMA
from matplotlib import pyplot
from pandas.plotting import autocorrelation_plot
from sklearn.metrics import mean_squared_error
from pymongo import MongoClient
import pymongo
from statsmodels.tsa.stattools import acf
import csv
import warnings
import itertools
import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt

def get_mongo_client():
    client = MongoClient("mongodb://localhost:27017/")
    return client

def parser(x):
    return datetime.strptime('190' + x, '%Y-%m')

client = get_mongo_client()
mydb = client["NEW_YORK_TAXI"]
mycol = mydb["month"]
cursor = mycol.find({'car_type': "green"},
                                    {"total_trips": 1, 'timestamp': 1, "_id": 0}).sort("timestamp", pymongo.ASCENDING)
res = []
previous_ts = 0


for doc in cursor:
    resp = doc
    if previous_ts == 0:
        # data = [0, float(resp[variable])]
        try:
            data = [resp['timestamp'], float(resp["total_trips"])]
            previous_ts = resp['timestamp']
        except:
            continue
    else:
        try:
            data = [resp['timestamp'], float(resp["total_trips"])]
        except:
            continue

    # resp.pop('_id', None)
    res.append(data)

print(res)

for i in res:
    i[0] = datetime.fromtimestamp(i[0])

df = pd.DataFrame(res)
df = df.set_index(0)
df.columns = ["Trips"]
df.plot()
print(df)
#Size of exchange rates
NumberOfElements = len(df)
# Create Training and Test
train = df.Trips[:40]
test = df.Trips[40:]

#Build Model
#model = ARIMA(train, order=(3,2,1))
model = ARIMA(train, order=(5, 1, 0))
fitted = model.fit(disp=0)

# Forecast
fc, se, conf = fitted.forecast(32, alpha=0.05)  # 95% conf
print(test.index)
print(type(test))
nueva = pd.Series([0],index=['2019-07-01'])
test = test.append(nueva)
print(test.shape)
# Make as pandas series
fc_series = pd.Series(fc, index=test.index)
lower_series = pd.Series(conf[:, 0], index=test.index)
upper_series = pd.Series(conf[:, 1], index=test.index)

# Plot
plt.figure(figsize=(12,5), dpi=100)
plt.plot(train, label='training')
plt.plot(test, label='actual')
plt.plot(fc_series, label='forecast')
plt.fill_between(lower_series.index, lower_series, upper_series,
                 color='k', alpha=.15)
plt.title('Forecast vs Actuals')
plt.legend(loc='upper left', fontsize=8)
plt.show()


def parser(x):
    return datetime.strptime('190' + x, '%Y-%m')


series = read_csv('shampoo-sales.csv', header=0, parse_dates=[0], index_col=0, squeeze=True, date_parser=parser)
X = df.values
size = int(len(X) * 0.66)
train, test = X[0:size], X[size:len(X)]
history = [x for x in train]
predictions = list()
for t in range(len(test)):
    model = ARIMA(history, order=(5, 1, 0))
    model_fit = model.fit(disp=0)
    output = model_fit.forecast()
    yhat = output[0]
    predictions.append(yhat)
    obs = test[t]
    history.append(obs)
    print('predicted=%f, expected=%f' % (yhat, obs))
error = mean_squared_error(test, predictions)
print('Test MSE: %.3f' % error)
# plot
pyplot.plot(test)
pyplot.plot(predictions, color='red')
pyplot.show()

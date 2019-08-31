from datetime import date,datetime
from flask import Flask, jsonify, request, session
from flask_cors import CORS, cross_origin
from pymongo import MongoClient
import pymongo
import pandas as pd
import json
import numpy as np
import os

from functools import wraps
app = Flask(__name__)
app.secret_key = 'U\x97\x8bl\xf9\xa7z\x01\xd0P\xf1\xbd\x03MH\xa8I\xbe\x93\x99\x87W\x0b\xdf'
CORS(app, supports_credentials=True)

class LoginSession:
    def __init__(self, username):
        self.username = username


def login_required(f):
    @wraps(f)
    def do_login(*args, **kwargs):
        try:
            ls = session['login']
        except:
            ls = None

        if ls is None:
            return "", 401

        return f(*args, **kwargs)

    return do_login

def get_mongo_client():
    client = MongoClient("mongodb://localhost:27017/")
    return client

@app.route("/do-logout/")
def do_logout():
    res = dict()
    res['result'] = 1
    session.clear()
    return jsonify(res)

@app.route("/taxi/do-login/")
def do_login_pla():
    res = dict()
    auth = request.authorization

    if auth.username == "taxi" and auth.password == "taxi":
        res['login'] = "Adelante!"
        res['result'] = 1
        ls = LoginSession(auth.username)
        session['login'] = vars(ls)

    return jsonify(res)

@app.route('/file/<name>')
@login_required
def process_file(name):
    if "yellow" in name:
        print("Parece que se va tratar el amarillo")
        yellow_file(name)
    elif "green" in name:
        print("Parece que se va tratar el verde")
        green_file(name)
    elif "fhv" in name:
        print("Parece que se va tratar el fhv")
        fhv_file(name)

    return jsonify("Saved")

@app.route('/now/<date>')
@login_required
def now(date):
    test = [date[i:i+4] for i in range(0, len(date), 4)]
    test2 = [date[i:i + 2] for i in range(0, len(date), 2)]
    fecha = datetime(int(test[0]), int(test2[2]), 1).timestamp()
    client = get_mongo_client()
    # Issue the serverStatus command and print the results
    mydb = client["NEW_YORK_TAXI"]
    mycol = mydb["month"]
    cursor = mycol.find({"timestamp": fecha, "car_type": "green"})
    res = {}
    for doc in cursor:
        res['green'] = doc
        res['green'].pop('_id', None)

    cursor = mycol.find({"timestamp": fecha, "car_type": "yellow"})
    for doc in cursor:
        res['yellow'] = doc
        res['yellow'].pop('_id', None)

    cursor = mycol.find({"timestamp": fecha, "car_type": "fhv"})
    for doc in cursor:
        res['fhv'] = doc
        res['fhv'].pop('_id', None)
        #respuesta = respuesta.append(res)
    return jsonify(res)

def fhv_file(name):
    df = pd.read_csv(r"C:\Users\109366\Desktop\TFM codigo\data\\fhv\\"+name, sep=",",
                     index_col=None).reset_index()
    name = name.replace('.csv','').split('_')

    try:
        df = df[['Pickup_DateTime', 'DropOff_datetime']] #para los amarillos
        df['Lpep_dropoff_datetime'] = pd.to_datetime(df['DropOff_datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')  #para los amarillos
        df['lpep_pickup_datetime'] = pd.to_datetime(df['Pickup_DateTime'], format='%Y-%m-%d %H:%M:%S', errors='coerce') # para los amarilos
    except:
        df = df[['Pickup_date']]  # para los amarillos
        df['lpep_pickup_datetime'] = pd.to_datetime(df['Pickup_date'], format='%Y-%m-%d %H:%M:%S',
                                                    errors='coerce')  # para los amarilos
    res = {}

    agrupado2 = df.groupby([df['lpep_pickup_datetime'].dt.hour]).count()
    Export = agrupado2.to_json(r'C:\Users\109366\Desktop\TFM codigo\data\fhv\Export2_DataFrame.json')
    with open(os.path.join(r'C:\Users\109366\Desktop\TFM codigo\data\fhv', 'Export2_DataFrame.json'), 'r') as f:
        jsonData = json.loads(f.read())
    res['agrupado2'] = jsonData
    count_row = df.shape[0]
    res['total_trips'] = int(count_row)
    try:
        df['Total_time'] = df['Lpep_dropoff_datetime'] - df['lpep_pickup_datetime']
        test = df.describe()
        test2 = test['Total_time']
        #res['total_trips'] = int(test2[0]) #antes contaba asi el total trips
        res['Total_time_avg'] = test2[1] / np.timedelta64(1, 'm')
        res['Total_time_std'] = test2[2] / np.timedelta64(1, 'm')
        res['Total_time_min'] = test2[3] / np.timedelta64(1, 'm')
        res['Total_time_max'] = test2[7] / np.timedelta64(1, 'm')
        res['Total_time_25'] = test2[4] / np.timedelta64(1, 'm')
        res['Total_time_50'] = test2[5] / np.timedelta64(1, 'm')
        res['Total_time_75'] = test2[6] / np.timedelta64(1, 'm')
    except:
        pass
    res['car_type'] = name[0]
    fecha = name[2].split('-')
    a = datetime(int(fecha[0]),int(fecha[1]),1)
    a = datetime.timestamp(a)
    res['timestamp'] = a

    client = get_mongo_client()
    # Issue the serverStatus command and print the results
    mydb = client["NEW_YORK_TAXI"]
    mycol = mydb["month"]
    mycol.insert_one(res)
def green_file(name):
    df = pd.read_csv(r"C:\Users\109366\Desktop\TFM codigo\data\\green\\"+name, sep=",",
                     index_col=None).reset_index()
    name = name.replace('.csv','').split('_')
    # column_names = df.columns[2:] #old files of green taxi
    # df = df.iloc[:, :-2] #old files of green taxi
    # df.columns = column_names #old files of green taxi
    #df = df[['Passenger_count', 'Trip_distance', 'Fare_amount', 'Total_amount','lpep_pickup_datetime','Lpep_dropoff_datetime']] #old files of green taxi
    df = df[['passenger_count', 'trip_distance', 'fare_amount', 'total_amount', 'lpep_pickup_datetime',
             'lpep_dropoff_datetime']]

    #df['Lpep_dropoff_datetime'] = pd.to_datetime(df['Lpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S') #old files of green taxi
    df['Lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S')
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
    res = {}
    agrupado = df.groupby([df['lpep_pickup_datetime'].dt.hour]).sum() #esto te saca datos interesantes, solo te importa passenger y distance
    Export = agrupado.to_json(r'C:\Users\109366\Desktop\TFM codigo\data\green\Export_DataFrame.json')
    with open(os.path.join(r'C:\Users\109366\Desktop\TFM codigo\data\green', 'Export_DataFrame.json'), 'r') as f:
        jsonData = json.loads(f.read())
    res['agrupado'] = jsonData
    agrupado2 = df.groupby([df['lpep_pickup_datetime'].dt.hour]).count()
    Export = agrupado2.to_json(r'C:\Users\109366\Desktop\TFM codigo\data\green\Export2_DataFrame.json')
    with open(os.path.join(r'C:\Users\109366\Desktop\TFM codigo\data\green', 'Export2_DataFrame.json'), 'r') as f:
        jsonData = json.loads(f.read())
    res['agrupado2'] = jsonData
    df['Total_time'] = df['Lpep_dropoff_datetime'] - df['lpep_pickup_datetime']
    test = df.describe()
    #test2 = test['Passenger_count'] #old files of green taxi
    test2 = test['passenger_count']

    res['total_trips'] = int(test2[0])
    res['passenger_avg'] = test2[1]
    res['passenger_std'] = test2[2]
    res['passenger_min'] = test2[3]
    res['passenger_max'] = test2[7]
    res['passenger_25'] = test2[4]
    res['passenger_50'] = test2[5]
    res['passenger_75'] = test2[6]
    #test2 = test['Trip_distance'] #old files of green taxi
    test2 = test['trip_distance']
    res['Trip_distance_avg'] = test2[1]
    res['Trip_distance_std'] = test2[2]
    res['Trip_distance_min'] = test2[3]
    res['Trip_distance_max'] = test2[7]
    res['Trip_distance_25'] = test2[4]
    res['Trip_distance_50'] = test2[5]
    res['Trip_distance_75'] = test2[6]
    #test2 = test['Fare_amount'] #old files of green taxi
    test2 = test['fare_amount']
    res['Fare_amount_avg'] = test2[1]
    res['Fare_amount_std'] = test2[2]
    res['Fare_amount_min'] = test2[3]
    res['Fare_amount_max'] = test2[7]
    res['Fare_amount_25'] = test2[4]
    res['Fare_amount_50'] = test2[5]
    res['Fare_amount_75'] = test2[6]
    #test2 = test['Total_amount'] #old files of green taxi
    test2 = test['total_amount']
    res['Total_amount_avg'] = test2[1]
    res['Total_amount_std'] = test2[2]
    res['Total_amount_min'] = test2[3]
    res['Total_amount_max'] = test2[7]
    res['Total_amount_25'] = test2[4]
    res['Total_amount_50'] = test2[5]
    res['Total_amount_75'] = test2[6]
    test2 = test['Total_time']
    res['Total_time_avg'] = test2[1] / np.timedelta64(1, 'm')
    res['Total_time_std'] = test2[2] / np.timedelta64(1, 'm')
    res['Total_time_min'] = test2[3] / np.timedelta64(1, 'm')
    res['Total_time_max'] = test2[7] / np.timedelta64(1, 'm')
    res['Total_time_25'] = test2[4] / np.timedelta64(1, 'm')
    res['Total_time_50'] = test2[5] / np.timedelta64(1, 'm')
    res['Total_time_75'] = test2[6] / np.timedelta64(1, 'm')
    res['car_type'] = name[0]
    fecha = name[2].split('-')
    a = datetime(int(fecha[0]),int(fecha[1]),1)
    a = datetime.timestamp(a)
    res['timestamp'] = a

    client = get_mongo_client()
    # Issue the serverStatus command and print the results
    mydb = client["NEW_YORK_TAXI"]
    mycol = mydb["month"]
    mycol.insert_one(res)

def yellow_file(name):
    df = pd.read_csv(r"C:\Users\109366\Desktop\TFM codigo\data\\yellow\\" + name, sep=",",
                     index_col=None).reset_index()
    name = name.replace('.csv', '').split('_')
    # column_names = df.columns[2:] #old files of green taxi
    # df = df.iloc[:, :-2] #old files of green taxi
    # df.columns = column_names #old files of green taxi
    # df = df[['Passenger_count', 'Trip_distance', 'Fare_amount', 'Total_amount','lpep_pickup_datetime','Lpep_dropoff_datetime']] #old files of green taxi
    df = df[['passenger_count', 'trip_distance', 'fare_amount', 'total_amount', 'tpep_pickup_datetime',
             'tpep_dropoff_datetime']]  # para los amarillos
    # df['Lpep_dropoff_datetime'] = pd.to_datetime(df['Lpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S') #old files of green taxi
    df['Lpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'],
                                                 format='%Y-%m-%d %H:%M:%S')  # para los amarillos
    df['lpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'],
                                                format='%Y-%m-%d %H:%M:%S')  # para los amarilos
    res = {}
    agrupado = df.groupby([df[
                               'lpep_pickup_datetime'].dt.hour]).sum()  # esto te saca datos interesantes, solo te importa passenger y distance
    Export = agrupado.to_json(r'C:\Users\109366\Desktop\TFM codigo\data\yellow\Export_DataFrame.json')
    with open(os.path.join(r'C:\Users\109366\Desktop\TFM codigo\data\yellow', 'Export_DataFrame.json'), 'r') as f:
        jsonData = json.loads(f.read())
    res['agrupado'] = jsonData
    agrupado2 = df.groupby([df['lpep_pickup_datetime'].dt.hour]).count()
    Export = agrupado2.to_json(r'C:\Users\109366\Desktop\TFM codigo\data\yellow\Export2_DataFrame.json')
    with open(os.path.join(r'C:\Users\109366\Desktop\TFM codigo\data\yellow', 'Export2_DataFrame.json'), 'r') as f:
        jsonData = json.loads(f.read())
    res['agrupado2'] = jsonData
    df['Total_time'] = df['Lpep_dropoff_datetime'] - df['lpep_pickup_datetime']
    test = df.describe()
    # test2 = test['Passenger_count'] #old files of green taxi
    test2 = test['passenger_count']
    res['total_trips'] = int(test2[0])
    res['passenger_avg'] = test2[1]
    res['passenger_std'] = test2[2]
    res['passenger_min'] = test2[3]
    res['passenger_max'] = test2[7]
    res['passenger_25'] = test2[4]
    res['passenger_50'] = test2[5]
    res['passenger_75'] = test2[6]
    # test2 = test['Trip_distance'] #old files of green taxi
    test2 = test['trip_distance']
    res['Trip_distance_avg'] = test2[1]
    res['Trip_distance_std'] = test2[2]
    res['Trip_distance_min'] = test2[3]
    res['Trip_distance_max'] = test2[7]
    res['Trip_distance_25'] = test2[4]
    res['Trip_distance_50'] = test2[5]
    res['Trip_distance_75'] = test2[6]
    # test2 = test['Fare_amount'] #old files of green taxi
    test2 = test['fare_amount']
    res['Fare_amount_avg'] = test2[1]
    res['Fare_amount_std'] = test2[2]
    res['Fare_amount_min'] = test2[3]
    res['Fare_amount_max'] = test2[7]
    res['Fare_amount_25'] = test2[4]
    res['Fare_amount_50'] = test2[5]
    res['Fare_amount_75'] = test2[6]
    # test2 = test['Total_amount'] #old files of green taxi
    test2 = test['total_amount']
    res['Total_amount_avg'] = test2[1]
    res['Total_amount_std'] = test2[2]
    res['Total_amount_min'] = test2[3]
    res['Total_amount_max'] = test2[7]
    res['Total_amount_25'] = test2[4]
    res['Total_amount_50'] = test2[5]
    res['Total_amount_75'] = test2[6]
    test2 = test['Total_time']
    res['Total_time_avg'] = test2[1] / np.timedelta64(1, 'm')
    res['Total_time_std'] = test2[2] / np.timedelta64(1, 'm')
    res['Total_time_min'] = test2[3] / np.timedelta64(1, 'm')
    res['Total_time_max'] = test2[7] / np.timedelta64(1, 'm')
    res['Total_time_25'] = test2[4] / np.timedelta64(1, 'm')
    res['Total_time_50'] = test2[5] / np.timedelta64(1, 'm')
    res['Total_time_75'] = test2[6] / np.timedelta64(1, 'm')
    res['car_type'] = name[0]
    fecha = name[2].split('-')
    a = datetime(int(fecha[0]), int(fecha[1]), 1)
    a = datetime.timestamp(a)
    res['timestamp'] = a

    client = get_mongo_client()
    # Issue the serverStatus command and print the results
    mydb = client["NEW_YORK_TAXI"]
    mycol = mydb["month"]
    mycol.insert_one(res)

    return jsonify("Saved")

@app.route("/car/<period>/<date>/")
@login_required
def get_mold_rest(period, date):
    test = [date[i:i + 4] for i in range(0, len(date), 4)]
    desde = datetime(int(test[0]), 1, 1).timestamp()
    hasta = datetime(int(test[0]) + 1, 1, 1).timestamp()
    vars = request.args.get('vars')
    mold = request.args.get('car')
    var_list = vars.split(',')
    mold_list = mold.split(',')
    client = get_mongo_client()
    # Issue the serverStatus command and print the results
    mydb = client["NEW_YORK_TAXI"]
    mycol = mydb["month"]
    data_json = []
    i = 1
    axis = {}
    for car in mold_list:
        for variable in var_list:
            if period == "all":
                test2 = [date[i:i + 2] for i in range(0, len(date), 2)]
                hasta = datetime(int(test[0]), int(test2[2]), int(test2[3])).timestamp()
                cursor = mycol.find({'timestamp': {'$gte': 1357002000, '$lte': hasta}, 'car_type': car},
                                    {variable: 1, 'timestamp': 1, "_id": 0}).sort("timestamp", pymongo.DESCENDING)
            else:
                cursor = mycol.find({'timestamp': {'$gte': desde, '$lte': hasta}, 'car_type': car},
                                    {variable: 1, 'timestamp': 1, "_id": 0}).sort("timestamp", pymongo.DESCENDING)
            previous_ts = 0
            res = []
            for doc in cursor:
                resp = doc
                if previous_ts == 0:
                    # data = [0, float(resp[variable])]
                    try:
                        data = [resp['timestamp'] * 1000, float(resp[variable])]
                        previous_ts = resp['timestamp']
                    except:
                        continue
                else:
                    # data = [resp['start']-previous_ts, float(resp[variable])]
                    try:
                        data = [resp['timestamp'] * 1000, float(resp[variable])]
                    except:
                        continue

                # resp.pop('_id', None)
                res.append(data)

            # print res
            if res:
                res_json = {}
                res_json['data'] = res
                res_json['label'] = variable+'_'+car
                data = variable.split("_")
                if data[0]+data[1] in axis:
                    res_json['yaxis'] = axis[data[0]+data[1]]
                else:
                    axis[data[0]+data[1]] = i
                    res_json['yaxis'] = i
                    i = i + 1

                data_json.append(res_json)

    return jsonify(data_json)

if __name__ == '__main__':
    #app.run(debug=True, host='0.0.0.0')
    app.run(host='0.0.0.0', port=8080)
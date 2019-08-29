from datetime import date,datetime
from flask import Flask, jsonify, request, session
from flask_cors import CORS, cross_origin
from pymongo import MongoClient
import pymongo
import pandas as pd
import json
import numpy as np

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
    df = pd.read_csv(r"C:\Users\109366\Desktop\TFM codigo\data\\"+name, sep=",",
                     index_col=None).reset_index()
    name = name.replace('.csv','').split('_')
    column_names = df.columns[2:]
    df = df.iloc[:, :-2]
    df.columns = column_names
    df = df[['Passenger_count', 'Trip_distance', 'Fare_amount', 'Total_amount','lpep_pickup_datetime','Lpep_dropoff_datetime']]
    #datetime_1 = datetime.strptime(df['Lpep_dropoff_datetime'], '%y-%m-%d %H:%M:%S')
    #datetime_2 = datetime.strptime(df['lpep_pickup_datetime'], '%y-%m-%d %H:%M:%S')
    #print((datetime_1 - datetime_2).datetime.minutes)
    df['Lpep_dropoff_datetime'] = pd.to_datetime(df['Lpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S')
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
    df['Total_time'] = df['Lpep_dropoff_datetime'] - df['lpep_pickup_datetime']
    test = df.describe()
    test2 = test['Passenger_count']
    res = {}
    res['total_trips'] = test2[0]
    res['passenger_avg'] = test2[1]
    res['passenger_std'] = test2[2]
    res['passenger_min'] = test2[3]
    res['passenger_max'] = test2[7]
    res['passenger_25'] = test2[4]
    res['passenger_50'] = test2[5]
    res['passenger_75'] = test2[6]
    test2 = test['Trip_distance']
    res['Trip_distance_avg'] = test2[1]
    res['Trip_distance_std'] = test2[2]
    res['Trip_distance_min'] = test2[3]
    res['Trip_distance_max'] = test2[7]
    res['Trip_distance_25'] = test2[4]
    res['Trip_distance_50'] = test2[5]
    res['Trip_distance_75'] = test2[6]
    test2 = test['Fare_amount']
    res['Fare_amount_avg'] = test2[1]
    res['Fare_amount_std'] = test2[2]
    res['Fare_amount_min'] = test2[3]
    res['Fare_amount_max'] = test2[7]
    res['Fare_amount_25'] = test2[4]
    res['Fare_amount_50'] = test2[5]
    res['Fare_amount_75'] = test2[6]
    test2 = test['Total_amount']
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
    #print(res)

    client = get_mongo_client()
    # Issue the serverStatus command and print the results
    mydb = client["NEW_YORK_TAXI"]
    mycol = mydb["month"]
    mycol.insert_one(res)
    #cursor = mycol.find()
    # cursor = mycol.find({"machine_part": "status"}).limit(1).sort("start",pymongo.DESCENDING)
    # for doc in cursor:
    #     res['status'] = doc
    #     res['status'].pop('_id', None)
    #     #respuesta = respuesta.append(res)
    #print respuesta
    return jsonify("Saved")



if __name__ == '__main__':
    #app.run(debug=True, host='0.0.0.0')
    app.run(host='0.0.0.0', port=8080)
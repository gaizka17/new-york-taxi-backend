import datetime
from flask import Flask, jsonify, request, session
import time
from flask_cors import CORS, cross_origin
import calendar
from calendar import monthrange
from pymongo import MongoClient
import json
import csv
import pymongo
import pandas as pd
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
    print(name)
    client = get_mongo_client()
    # Issue the serverStatus command and print the results
    mydb = client["NEW_YORK_TAXI"]
    mycol = mydb["month"]
    res = {}
    df = pd.read_csv(r"C:\Users\gaizka\Desktop\UNIR\GAIZKA\ASIGNATURAS\2 Semestre\TFM\data\\"+name, sep=',')
    #print(data.dtypes)
    #print(df.head(n=10))
    print(df.describe())
    #print(data[0])
    #print(data[1])
    #print(data['VendorID'])
    #print(data['Payment_type'])
    # mean1 = data['Total_amount'].mean()
    # print(mean1)
    #cursor = mycol.find()
    # cursor = mycol.find({"machine_part": "status"}).limit(1).sort("start",pymongo.DESCENDING)
    # for doc in cursor:
    #     res['status'] = doc
    #     res['status'].pop('_id', None)
    #     #respuesta = respuesta.append(res)
    #print respuesta
    return jsonify(res)



if __name__ == '__main__':
    #app.run(debug=True, host='0.0.0.0')
    app.run(host='0.0.0.0', port=8080)
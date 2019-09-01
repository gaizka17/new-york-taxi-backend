# -*- coding: utf-8 -*-
import os
import csv
import json
import numpy as np
import rest

def proccess_files():
    #folder_path = "C:\Users\\109366\Desktop\GAIZKA\proyectos\GKN\data"
    folder_path = r"C:\Users\109366\Desktop\TFM codigo\data\yellow"
    for root, dirs, files in os.walk(folder_path, topdown=False):
            for name in files:
                # exists = os.path.isfile(folder_path+"\Processed\\"+name.replace(".csv",".json"))
                # if not exists:
                if name.endswith(".csv"):
                    convert_file(name)

def convert_file(name):
    print("Se va tratar:", name)
    rest.yellow_file(name)

def main():
    proccess_files()



if __name__ == "__main__":
    main()

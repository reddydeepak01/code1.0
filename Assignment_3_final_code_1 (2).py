#!/usr/bin/env python
# coding: utf-8

# In[2]:


#importing all the required modules
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import datetime
from pymongo import MongoClient


Connecting to MongoDB
Mongodb_host = 'localhost'
Mongodb_port = 27017
db_name = 'ForexData'
collection_name = 'ForexPairs'

client = MongoClient(Mongodb_host, Mongodb_port)
conn = client[db_name][collection_name]


db_mongo = client['forexDataFinal']
collection = db_mongo['forexpairsFinal']

Connecting to MySQL 
db = mysql.connector.connect(host = "localhost",
                             #user = "root",
                             #passwd = "sdeereddy")

c = db.cursor()

database_created = 'Final_forex_database_2'

c.execute(f"CREATE DATABASE {database_created}")

engine = create_engine(f"mysql+pymysql://root:sdeereddy@localhost/{database_created}")

#all the 62 forex pairs looped to store in MySQL and MongoDB and convert to .csv
forex_pairs = ['AUDCHF','AUDCAD','AUDHKD','AUDJPY','AUDNZD','AUDSGD','CADCHF','CADHKD','CADJPY','CADSGD',
               'CHFHKD','CHFJPY','CHFZAR','EURAUD','EURCAD','EURCHF','EURCZK','EURDKK','EURGBP','EURHKD',
               'EURHUF','EURJPY','EURNOK','EURPLN','EURSEK','EURTRY','EURUSD','EURZAR','GBPAUD','GBPCAD',
               'GBPCHF','GBPHKD','GBPJPY','GBPNZD','GBPPLN','GBPUSD','HKDJPY','NZDCAD','NZDCHF','NZDHKD',
               'NZDJPY','NZDSGD','NZDUSD','SGDCHF','USDBRL','USDCHF','USDCNY','USDCZK','USDDKK','USDHKD','USDHUF',
               'USDJPY','USDMXN','USDNOK','USDPLN','USDSEK','USDSGD','USDTHB','USDTRY','USDZAR','ZARJPY']

for pair in forex_pairs:
    
    df = pd.read_csv(f"C:\\Users\\deepak\\OneDrive\\Desktop\\Homework_3_data\\{pair}.csv")

    cols =['_id','v','o','c','h','l','datetime']

    df.drop(cols, inplace = True, axis =1)

    df['t'] = pd.to_datetime(df['t'], unit='ms')

    df['X'] = df['vw']*df['n']

    start_date = '2011-07-12'
    end_date = '2023-01-24'
    
    #filter all the rows in the sourcefile which fall below the start date to ensure same starting date
    #and filter out the rows greated than the enddate to ensure same ending date for all the currecny pairs
    filtered =(df['t'] > start_date) & (df['t'] <= end_date)        

    df_3 = df.loc[filtered]
    
    #using resample() method to compress the minutes data to 1hour
    df_3.resample('60min', on = 't').agg({"vw": 'mean', "n": 'sum'})
    df_3 = df_3.fillna(method="ffill")
    
    #using resample() on the resampled data to compress the 1hour data to 6hours
    df_compressed = df_3.resample('6H', on='t').agg(VWAP = pd.NamedAgg(column='vw', aggfunc=pd.Series.mean),Liquidity = pd.NamedAgg(column='n', aggfunc=pd.Series.mean),Max = pd.NamedAgg(column='vw', aggfunc=pd.Series.max),Min = pd.NamedAgg(column='vw', aggfunc=pd.Series.min))
    
    #Calculating the VWAP on the compressed data and storing in a dataframe to be stored in the database
    df_compressed['Timestamp'] = df_compressed.index
    df_compressed["Volatility"] = (df_compressed["Max"] - df_compressed["Min"]) / df_compressed["VWAP"]

    dictionary = df_2.to_dict("records")
    
    conn.insert_many(dictionary, ordered = False)
    
    df_2.to_sql(f"{pair.lower()}_final", engine, index = False)
    
    #storing the dataframes as .csv files
    df_compressed.to_csv(f"C:\\Users\\deepak\\OneDrive\\Desktop\\Homework_3_data\\Final_pres\\{pair}_final.csv", index = False)
    
    print(f'data inserted for {pair}')




# In[5]:


import pandas as pd
import datetime
import pycaret
from pycaret.regression import *

#resgression analysis of all the 62 currency pairs.
forex_pairs = ['AUDCHF','AUDCAD','AUDHKD','AUDJPY','AUDNZD','AUDSGD','CADCHF','CADHKD','CADJPY','CADSGD',
               'CHFHKD','CHFJPY','CHFZAR','EURAUD','EURCAD','EURCHF','EURCZK','EURDKK','EURGBP','EURHKD',
               'EURHUF','EURJPY','EURNOK','EURPLN','EURSEK','EURTRY','EURUSD','EURZAR','GBPAUD','GBPCAD',
               'GBPCHF','GBPHKD','GBPJPY','GBPNZD','GBPPLN','GBPUSD','HKDJPY','NZDCAD','NZDCHF','NZDHKD',
               'NZDJPY','NZDSGD','NZDUSD','SGDCHF','USDBRL','USDCHF','USDCNY','USDCZK','USDDKK',
               'USDHKD','USDHUF','USDJPY','USDMXN','USDNOK','USDPLN','USDSEK','USDSGD','USDTHB','USDTRY','USDZAR','ZARJPY']

for pair in forex_pairs:
    dataset = pd.read_csv(f"C:\\Users\\deepak\\OneDrive\\Desktop\\Homework_3_data\\Final_pres\\{pair}_final.csv")
    dataset = dataset.dropna()
    
    print(f'regression for {pair} started')
    
    #setup by giving the target variable for the module to generate regression models
    regression_setup = setup(data = dataset, target = 'VWAP', train_size=0.7)
    best = compare_models()
    
    plot_model(best)
    
    predictions = predict_model(best, data = dataset)
    
    predictions

    


# In[ ]:





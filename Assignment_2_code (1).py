#!/usr/bin/env python
# coding: utf-8

# In[1]:


#could do only 38 iterations, the code stopped due to system inactivity and later the markets closed.
#importing the required modules
import requests
import sqlalchemy
import json
import time
import sqlite3
import pandas as pd
from datetime import datetime
import concurrent.futures

engine = sqlalchemy.create_engine('sqlite:///Forex_final.db')

forex_pairs = ['USDAUD', 'USDJPY', 'USDEUR']

df = pd.DataFrame(columns=['timestamp', 'Fx_rate', 'timestamp_of_entry'])
aux_df = pd.DataFrame(columns=['Period','Max', 'Min', 'Mean', 'VOL'])

final_df = pd.DataFrame(columns = ['Entry_time', 'Max','Min','Mean','VOL','FD','Period'])

period_duration = 6*60

#pulling the data from polygon.io and storing it in a dataframe
def fxrate(pair):
    API_key = f'https://api.polygon.io/v1/conversion/{pair[:3]}/{pair[3:]}?amount=1&precision=4&apiKey=beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq'
    response = requests.get(API_key)
    data = response.json()

    ask = data['last']['ask']
    bid = data['last']['bid']
    Fx_rate = (ask + bid) / 2
    timestamp = data['last']['timestamp']
    timestamp_of_entry = datetime.fromtimestamp(time.time())

    record = pd.DataFrame({'timestamp': timestamp, 'Fx_rate': Fx_rate, 'timestamp_of_entry': timestamp_of_entry}, index=[0])
    return record

#calculating the keltner channels and number of crossing by accessing the tables created
#pulling the data from the tables and then performing the required calculation
def calculation_keltner_bands(pair):
    
    df_1 = pd.read_sql_table(f'aux_data_{pair}', engine, columns =['Mean','VOL'])
    
    previous_mean = df_1.iloc[0]['Mean']
    previous_VOL = df_1.iloc[0]['VOL']
    
    keltner_upper_bands = [previous_mean + n * 0.025 * previous_VOL for n in range(1, 101)]
    keltner_lower_bands = [previous_mean - n * 0.025 * previous_VOL for n in range(1, 101)]
    
    df_2 = pd.read_sql_table(f'temp_data{pair}', engine, columns = ['Fx_rate','timestamp_of_entry'])
    
    rate = df_2.iloc[0]['Fx_rate']
    
    number_of_crossings = sum(1 for _ in range(100) if float(keltner_upper_bands[_]< rate or rate < keltner_lower_bands[_]))
    
    return number_of_crossings
    
#to process each currency pair
# 6 min loop for 50 iterations to calculate max, min, mean, VOL and FD
# and store in a dataframe and then finally to a sql table
def process_fx_rate(fx_pair):
 
    for period in range(1, 51):
        
        period_end = time.time() + period_duration
        rates = []
        while time.time() < period_end:
            result = fxrate(fx_pair)
            rates.append(result)
            time.sleep(1)

        df_temp = pd.concat(rates)
        df_temp.to_sql(f'temp_data{fx_pair}', engine, if_exists = 'replace', index = False)
        df_rate = df_temp['Fx_rate']
        # print(df)

        max_rate = df_rate.max()
        min_rate = df_rate.min()
        mean_rate = df_rate.mean()
        VOL = (max_rate - min_rate) / mean_rate
        
        Final_data = []
        entry_time = datetime.fromtimestamp(time.time())
        if period > 1:
            N = calculation_keltner_bands(fx_pair)
            
            FD = N/(max_rate - min_rate)
            print (FD)
    
            Final_cal = pd.DataFrame({'Entry_time': entry_time,'Max': max_rate,'Min':min_rate,'Mean':mean_rate,'VOL': VOL,'FD':FD,'Period': period}, index = [0])
            Final_data.append(Final_cal)

            final_df = pd.concat(Final_data)
            final_df.to_sql(f'Final_data_{fx_pair}', engine, if_exists = 'append', index = False)

        p_data = []

        period_data = pd.DataFrame({'Period':period,'Max': max_rate, 'Min': min_rate, 'Mean': mean_rate, 'VOL': VOL}, index=[0])
        p_data.append(period_data)
        
        aux_df = pd.concat(p_data)
        aux_df.to_sql(f'aux_data_{fx_pair}', engine, if_exists='replace', index=False)
        
        print(f'The max, min, mean, and VOL of period {period} {fx_pair} are {max_rate}, {min_rate}, {mean_rate}, and {VOL}')
    
    
engine.dispose()
# multithread to execute all the 3 pairs at the same time.
with concurrent.futures.ThreadPoolExecutor() as executor:
    end = executor.map(process_fx_rate, forex_pairs)


# In[ ]:





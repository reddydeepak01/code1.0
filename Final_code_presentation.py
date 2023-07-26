#!/usr/bin/env python
# coding: utf-8

# In[47]:


#importing all modules
import requests
import sqlalchemy
import json
import time as t
import sqlite3
import pandas as pd
from datetime import datetime as dt
from collections import deque

#input statement to give the Forex pairs
forex_pair = input('Enter the forex pair: ')
forex_pairs =forex_pair[:6]
trade_position = forex_pair[7:]

#Connecting to db and creating the tables
engine = sqlalchemy.create_engine(f'sqlite:///Final_presentation_test_4.db')

engine.execute(f"CREATE TABLE IF NOT EXISTS 'Final_table {forex_pairs}'(rate FLOAT, unit INT, execution INT)")
engine.execute(f"CREATE TABLE IF NOT EXISTS 'Final_cal_table {forex_pairs}'(execution VARCHAR(50), average FLOAT, totalexecution INT, totalnonexecution INT)")
engine.execute(f"CREATE TABLE IF NOT EXISTS 'PL_table {forex_pairs}'(rate FLOAT, unit INT, rate_x_unit FLOAT ,execution INT)")

#forex_pairs = 'EURZAR'

#input statement to give the execution windows and units
execution_windows = deque([])
unit_list = []
for _ in range(1,5):
    y = (input('Enter the execution window and the units to be executed: '))
    start_time = int(y[:4])%100
    end_time = int(y[5:9])%100
    units = int(y[10:])
    z = (start_time,end_time)
    execution_windows.append(z)
    unit_list.append(units)

exec_1 = execution_windows[0]
exec_2 = execution_windows[1]
exec_3 = execution_windows[2]
exec_4 = execution_windows[3]

print(unit_list)
print(execution_windows)


# In[48]:


#function to extract data from polygon.io
def fxrate(pair):
    API_key = f'https://api.polygon.io/v1/conversion/{pair[:3]}/{pair[3:]}?amount=1&precision=4&apiKey=beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq'
    response = requests.get(API_key)
    data = response.json()

    ask = data['last']['ask']
    bid = data['last']['bid']
    Fx_rate = (ask + bid) / 2
    timestamp = data['last']['timestamp']
    timestamp_of_entry = dt.fromtimestamp(t.time())

    record = pd.DataFrame({'timestamp': timestamp, 'Fx_rate': Fx_rate, 'timestamp_of_entry': timestamp_of_entry}, index=[0])
    return record


# In[49]:


#The main executing function
def process_fx_rate(fx_pair):
    
    while execution_windows:
        time_range = execution_windows.popleft()
        
        alert = True
        while not dt.now().minute / time_range[0] >= 1 and dt.now().minute / time_range[1]<1:
            if alert:
                print("[DEBUG] Will wait for the right hour to start the execution")
                alert = False
        
        if time_range == exec_1:
            x = unit_list[0]
            exec_units = x
            execution = 1
        if time_range == exec_2:
            y = unit_list[1]
            exec_1_units = y
            execution = 2
        if time_range == exec_3:
            y = unit_list[2] + unexecuted
            exec_1_units = y
            execution = 3
        if time_range == exec_4:
            y = unit_list[3] + unexecuted
            exec_1_units = y
            execution = 4
 
        time_period = time_range[1]-time_range[0]        
        while dt.now().minute / time_range[0] >= 1 and dt.now().minute / time_range[1]<1:
            print('the loop starts at:', dt.fromtimestamp(t.time()))
            if time_range == exec_1:
                for _ in range(1,61):
                    result = fxrate(fx_pair)
                    rate = result.iloc[0]['Fx_rate']
                    #print(f'the forex pair {fx_pair} rate is: {rate} at time ',dt.fromtimestamp(t.time()))
                    t.sleep(1)
                    if _ == 60:
                        unit = x//time_period
                        rate_x_unit = rate * unit
                        exec_units = exec_units - unit
                        print(f"we executed {unit} units at {rate} at time: ",dt.fromtimestamp(t.time()))
                        print(f"the new units are {exec_units}")
                        engine.execute(f"INSERT INTO 'Final_table {forex_pairs}' (rate, unit, execution) VALUES (?, ?, ?)", rate, unit, execution)
                        engine.execute(f"INSERT INTO 'PL_table {forex_pairs}' (rate, unit, rate_x_unit ,execution) VALUES (?, ?, ?, ?)", rate, unit, rate_x_unit, execution)
                
            else:
                print(f'this is {execution} loop')
                for _ in range(1,61):
                    result = fxrate(fx_pair)
                    rate = result.iloc[0]['Fx_rate']
                    t.sleep(1)
                    if _ == 60:
                        if trade_position == 'L':
                            if rate >= avg_:
                                unit = y//time_period
                                rate_x_unit = rate * unit
                                exec_1_units = exec_1_units - unit
                                print(f"we executed {unit} units at {rate} at time: ",dt.fromtimestamp(t.time()))
                                print(f"the new units are {exec_1_units}")
                                print(f"the average is {avg_}")
                                engine.execute(f"INSERT INTO 'Final_table {forex_pairs}' (rate, unit, execution) VALUES (?, ?, ?)", rate, unit, execution)
                                engine.execute(f"INSERT INTO 'PL_table {forex_pairs}' (rate, unit, rate_x_unit ,execution) VALUES (?, ?, ?, ?)", rate, unit, rate_x_unit, execution)
                                
                        if trade_position == 'S':
                            if rate <= avg_:
                                unit = y//time_period
                                rate_x_unit = rate * unit
                                exec_1_units = exec_1_units - unit
                                print(f"we executed {unit} units at {rate} at time: ",dt.fromtimestamp(t.time()))
                                print(f"the new units are {exec_1_units}")
                                print(f"the average is {avg_}")
                                engine.execute(f"INSERT INTO 'Final_table {forex_pairs}' (rate, unit, execution) VALUES (?, ?, ?)", rate, unit, execution)
                                engine.execute(f"INSERT INTO 'PL_table {forex_pairs}' (rate, unit, rate_x_unit ,execution) VALUES (?, ?, ?, ?)", rate, unit, rate_x_unit, execution)

        if execution == 1:
            result_exec_avg = engine.execute(f"SELECT AVG(rate) FROM 'Final_table {forex_pairs}' WHERE execution = 1")
            avg = result_exec_avg.fetchone()[0]
            avg_ = avg
            print(f'the average of first execution is {avg}')
        if execution == 2:
            result_exec_avg = engine.execute(f"SELECT AVG(rate) FROM 'Final_table {forex_pairs}' WHERE execution = 1 OR execution = 2")
            avg = result_exec_avg.fetchone()[0]
            avg_ = avg
            print(f'the average of second execution is {avg}')
            unexecuted = exec_1_units
        if execution == 3:
            result_exec_avg = engine.execute(f"SELECT AVG(rate) FROM 'Final_table {forex_pairs}' WHERE execution = 1 OR execution = 2 OR execution = 3")
            avg = result_exec_avg.fetchone()[0]
            avg_ = avg
            print(f'the average of third execution is {avg}')
            unexecuted = exec_1_units
        if execution == 4:
            result_exec_avg = engine.execute(f"SELECT AVG(rate) FROM 'Final_table {forex_pairs}'")
            avg = result_exec_avg.fetchone()[0]
            avg_ = avg
            print(f'the average of fourth execution is {avg}')
            unexecuted = exec_1_units
            


#calling the fucntion
process_fx_rate(forex_pairs)

closing_windows = deque([])

#input statement to give closing window time range        
y = (input('Enter the execution window and the units to be executed: '))
start_time = int(y[:4])%100
end_time = int(y[5:9])%100
z = (start_time,end_time)
closing_windows.append(z)

print(closing_windows)

exec_5 = closing_windows[0]

while closing_windows:
    
    time_range = closing_windows.popleft()

    alert = True
    while not dt.now().minute / time_range[0] >= 1 and dt.now().minute / time_range[1]<1:
        if alert:
            print("[DEBUG] Will wait for the right hour to start the execution")
            alert = False

    result_exec_units = engine.execute(f"SELECT COALESCE(SUM(unit),0) FROM 'Final_table {forex_pairs}' WHERE execution = 1 OR execution = 2 OR execution = 3 OR execution =4")

    x = result_exec_units.fetchone()[0]
    if time_range == exec_5:
        exec_2_units = x
        execution = 5
        
    time_period = time_range[1]-time_range[0]
    while dt.now().minute / time_range[0] >= 1 and dt.now().minute / time_range[1]<1:
        print('the loop starts at:', dt.now().time())
        if exec_2_units != 0:
            for _ in range(1,61):
                result = fxrate(forex_pairs)
                rate = result.iloc[0]['Fx_rate']
                t.sleep(1)
                if _ == 60:
                    unit = x//time_period
                    rate_x_unit = rate * unit
                    exec_2_units = exec_2_units - unit
                    print(f"we executed {unit} units at {rate} at time: ",dt.fromtimestamp(t.time()))
                    print(f"the new units are {exec_2_units}")
                    print(f"the rate * units is {rate_x_unit}")
                    engine.execute(f"INSERT INTO 'Final_table {forex_pairs}' (rate, unit, execution) VALUES (?, ?, ?)", rate, unit, execution)
                    engine.execute(f"INSERT INTO 'PL_table {forex_pairs}' (rate, unit, rate_x_unit ,execution) VALUES (?, ?, ?, ?)", rate, unit, rate_x_unit, execution)
        else:
            break



# In[45]:


#calculations for the each individual execution
for i in range(1,6):
    result_exec_avg = engine.execute(f"SELECT COALESCE(AVG(rate),0) FROM 'Final_table {forex_pairs}' WHERE execution = {i}")
    result_exec_units = engine.execute(f"SELECT COALESCE(SUM(unit),0) FROM 'Final_table {forex_pairs}' WHERE execution = {i}")
    average = result_exec_avg.fetchone()[0]
    total_execution = result_exec_units.fetchone()[0]
    if i == 1:
        total_non_executed_1 =  unit_list[0] - total_execution
        engine.execute(f"INSERT INTO 'Final_cal_table {forex_pairs}' (execution, average,totalexecution,totalnonexecution ) VALUES (?, ?, ?, ?)", i, average, total_execution,total_non_executed_1)
    if i == 2:
        total_non_executed_2 =  unit_list[1] - total_execution
        engine.execute(f"INSERT INTO 'Final_cal_table {forex_pairs}' (execution, average,totalexecution,totalnonexecution ) VALUES (?, ?, ?, ?)", i, average, total_execution,total_non_executed_2)
    if i == 3:
        total_non_executed_3 = (unit_list[2] + total_non_executed_2) - total_execution
        engine.execute(f"INSERT INTO 'Final_cal_table {forex_pairs}' (execution, average,totalexecution,totalnonexecution ) VALUES (?, ?, ?, ?)", i, average, total_execution,total_non_executed_3)
    if i == 4:
        total_non_executed_4 = (unit_list[3] + total_non_executed_3) - total_execution
        engine.execute(f"INSERT INTO 'Final_cal_table {forex_pairs}' (execution, average,totalexecution,totalnonexecution ) VALUES (?, ?, ?, ?)", i, average, total_execution,total_non_executed_4)
    if i == 5:
        result_exec_units = engine.execute(f"SELECT COALESCE(SUM(unit),0) FROM 'Final_table {forex_pairs}' WHERE execution = 1 OR execution = 2 OR execution = 3 OR execution = 4")
        total_execution_4 = result_exec_units.fetchone()[0]
        result_exec_units_5 = engine.execute(f"SELECT COALESCE(SUM(unit),0) FROM 'Final_table {forex_pairs}' WHERE execution = 5")
        total_execution = result_exec_units_5.fetchone()[0]
        total_non_executed_5 = total_execution_4 - total_execution
        engine.execute(f"INSERT INTO 'Final_cal_table {forex_pairs}' (execution, average,totalexecution,totalnonexecution ) VALUES (?, ?, ?, ?)", i, average, total_execution,total_non_executed_5)
    print(f'the average of {i} execution is {average}')
    print(f'the total units executed in {i} execution are {total_execution}')

#Overall execution values
result_exec_avg_overall = engine.execute(f"SELECT AVG(rate) FROM 'Final_table {forex_pairs}' WHERE execution = 1 OR execution = 2 OR execution = 3 OR execution = 4")
result_exec_units_overall = engine.execute(f"SELECT SUM(unit) FROM 'Final_table {forex_pairs}' WHERE execution = 1 OR execution = 2 OR execution = 3 OR execution = 4")
average_overall = result_exec_avg_overall.fetchone()[0]
total_execution_overall = result_exec_units_overall.fetchone()[0]
total_non_executed_overall = sum(unit_list) - total_execution_overall
print(f'the avergae of overall executions is {average_overall}')
print(f'the total units executed in overall executions are {total_execution_overall}')
engine.execute(f"INSERT INTO 'Final_cal_table {forex_pairs}' (execution, average,totalexecution,totalnonexecution ) VALUES (?, ?, ?, ?)", 'overall', average_overall, total_execution_overall,total_non_executed_overall)


# In[24]:


#Overall execution values
first_4_executions = engine.execute(f"SELECT SUM(rate_x_unit) FROM 'PL_table {forex_pairs}' WHERE execution = 1 OR execution = 2 OR execution = 3 OR execution = 4")
buy_sell_first_4 = first_4_executions.fetchone()[0]
closing_execution = engine.execute(f"SELECT SUM(rate_x_unit) FROM 'PL_table {forex_pairs}' WHERE execution = 5")
closing_buy_sell = closing_execution.fetchone()[0]

profit_or_loss = closing_buy_sell - buy_sell_first_4

print(profit_or_loss)


# In[25]:


df_1 = pd.read_sql(f"SELECT * FROM 'PL_table {forex_pairs}'",engine)
df_1.to_csv(f"C:\\Users\\deepak\\OneDrive\\Desktop\\Academics\\Data Engineering\\Final presentation\\{forex_pairs}_PL.csv")
df_2 = pd.read_sql(f"SELECT * FROM 'Final_cal_table {forex_pairs}'",engine)
df_2.to_csv(f"C:\\Users\\deepak\\OneDrive\\Desktop\\Academics\\Data Engineering\\Final presentation\\{forex_pairs}_final.csv")


# In[ ]:





#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import numpy as np
import matplotlib as plt
import seaborn as sns


# In[2]:


file_path = r"C:\\Users\\deepak\\OneDrive\\Desktop\\Data Science projects\\train.csv"

df = pd.read_csv(file_path)


# In[3]:


df


# In[4]:


df.replace("?", np.nan, inplace = True)
df.head(5)


# In[5]:


missing_data = df.isnull()
missing_data.head(5)


# In[6]:


for column in missing_data.columns.values.tolist():
    print(column)
    print (missing_data[column].value_counts())
    print("")


# In[7]:


df.dtypes


# In[8]:


avg_imbalance = df['imbalance_size'].astype('float').mean()
print('average of the imbalance size:', avg_imbalance)


# In[9]:


df['imbalance_size'].replace(np.nan, avg_imbalance, inplace = True)


# In[10]:


avg_reference = df['reference_price'].astype('float').mean()
print('average of the reference price:', avg_reference)


# In[11]:


df['reference_price'].replace(np.nan, avg_reference, inplace = True)


# In[12]:


avg_matched = df['matched_size'].astype('float').mean()
print('avg of matched size:', avg_matched)


# In[13]:


df['matched_size'].replace(np.nan, avg_matched, inplace = True)


# In[14]:


avg_farprice = df['far_price'].astype('float').mean()
print('average of far price:', avg_farprice)


# In[15]:


df.drop('far_price', axis = 1, inplace = True)


# In[16]:


df.drop('near_price', axis = 1, inplace = True)


# In[17]:


avg_bidprice = df['bid_price'].astype('float').mean()
print('average of bid price:', avg_bidprice)


# In[18]:


df['bid_price'].replace(np.nan, avg_bidprice, inplace = True)


# In[19]:


avg_askprice = df['ask_price'].astype('float').mean()
print('average of ask price:', avg_askprice)


# In[20]:


df['ask_price'].replace(np.nan, avg_askprice, inplace = True)


# In[21]:


df['wap'].value_counts()


# In[22]:


avg_wap = df['wap'].astype('float').mean()


# In[23]:


print('average wap:', avg_wap)


# In[24]:


df['wap'].replace(np.nan, avg_wap, inplace = True)


# In[25]:


avg_target = df['target'].astype('float').mean()


# In[26]:


print('average target:', avg_target)


# In[27]:


df['target'].replace(np.nan, avg_target, inplace = True)


# In[28]:


missing_data_check = df.isnull()
missing_data_check.head(5)


# In[29]:


for column in missing_data_check.columns.values.tolist():
    print(column)
    print (missing_data_check[column].value_counts())
    print("")


# In[30]:


df.dtypes


# In[31]:


df['row_id'].value_counts()


# In[32]:


df['time_id'].value_counts()


# In[33]:


df['imbalance_buy_sell_flag'].value_counts()


# In[34]:


df


# In[35]:


df['price'] = (df['ask_price']+df['bid_price'])/2


# In[36]:


df


# In[37]:


df[['price','reference_price', 'bid_price', 'bid_size', 'ask_price', 'ask_size', 'wap', 'target']].corr()


# In[38]:


df.to_csv(f"C:\\Users\\deepak\\OneDrive\\Desktop\\Data Science projects\\train_cleaned.csv")


# In[38]:


import matplotlib.pyplot as plt
plt.scatter(df[['price']],df[['reference_price']])
plt.show()


# In[40]:


plt.scatter(df[['price']],df[['bid_price']])
plt.show()


# In[46]:


plt.scatter(df[['price']],df[['bid_size']])
plt.show()


# In[47]:


plt.scatter(df[['price']],df[['ask_price']])
plt.show()


# In[48]:


plt.scatter(df[['price']],df[['ask_size']])
plt.show()


# In[49]:


plt.scatter(df[['price']],df[['wap']])
plt.show()


# In[50]:


plt.scatter(df[['price']],df[['target']])
plt.show()


# In[54]:


get_ipython().system('pip install -U scikit-learn')


# In[39]:


import sklearn


# In[40]:


from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import r2_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error


# In[41]:


from sklearn.linear_model import Ridge, Lasso, ElasticNet, LassoLars, OrthogonalMatchingPursuit
from sklearn.svm import SVR, NuSVR, LinearSVR
from sklearn.neighbors import KNeighborsRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, AdaBoostRegressor, BaggingRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.gaussian_process import GaussianProcessRegressor


# In[42]:


from sklearn.pipeline import Pipeline


# In[43]:


#We create the pipeline by creating a list of tuples including the name of the model or estimator and its corresponding constructor.
Input=[('scale',StandardScaler()), ('polynomial', PolynomialFeatures(include_bias=False)), ('model',LinearRegression())]

#We input the list as an argument to the pipeline constructor:
pipe=Pipeline(Input)


# In[44]:


Z = df[['reference_price', 'bid_size', 'ask_size', 'wap', 'target']]
y = df['price']
Z = Z.astype(float)
f = pipe.fit(Z,y)

ypipe=pipe.predict(Z)
ypipe[0:20]


# In[45]:


r_squared = r2_score(y, ypipe)
print('The R-square value is: ', r_squared)


# In[46]:


mse = mean_squared_error(df['price'], ypipe)
print('The mean square error of price and predicted value is: ', mse)


# In[48]:


width = 12
height = 10
plt.figure(figsize=(width, height))


ax1 = sns.distplot(df['price'], hist=False, color="r", label="Actual Value")
sns.distplot(ypipe, hist=False, color="b", label="Fitted Values" , ax=ax1)


plt.title('Actual vs Fitted Values for Price')
plt.xlabel('Price (in dollars)')
plt.ylabel('Proportion of Cars')

plt.show()
plt.close()


# In[ ]:


r_squared = r2_score(y, ypipe)
print('The R-square value is: ', r_squared)


# In[ ]:


mse = mean_squared_error(df['price'], ypipe)
print('The mean square error of price and predicted value is: ', mse)


# In[ ]:





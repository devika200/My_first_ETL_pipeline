import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_csv('bank_transactions_data_2.csv')
print('before',"\n",df.head())

#PREPROCESSING

#1.standardize column names
df.columns=df.columns.str.strip().str.lower().str.replace(" ","_")
print(df.columns.tolist())

#2. fixing data types

df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
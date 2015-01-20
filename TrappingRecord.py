import sqlalchemy
import pandas as pd
import numpy as np
import matplotlib as plt
import json

def retrieve_data(query):
    """Given a query, this function opens a connection to the Portal Rodent 
    Database on Serenitym executes the mySQL query and returns the data in a 
    Pandas dataframe. This function is currently constructed to work for a 
    specific user and only if the JSON file "db_credentials" for accessing the 
    database is in the same folder"""
    credentials = json.load(open("db_credentials.json", "r"))
    engine = sqlalchemy.create_engine('mysql+pymysql://morgan:{}@{}:{}/{}'.format(credentials['password'], credentials['host'], credentials['port'], credentials['database']))
    data=pd.read_sql_query(query, engine)
    return data

#Load Data and and Add 1 to denote that an existing record represents a trapping event
"""The query excludes negative periods and records with notes that indicate that
the data was not from a normal census"""
query_rats = """SELECT Rodents.period, Rodents.plot, Rodents.note1 FROM Rodents 
                WHERE (((Rodents.period)>0 AND Rodents.period < 429) AND ((Rodents.note1) Is Null 
                Or (Rodents.note1)="1" Or (Rodents.note1)="2" 
                Or (Rodents.note1)="3" Or (Rodents.note1)="6" 
                Or (Rodents.note1)="7" Or (Rodents.note1)="10" 
                Or (Rodents.note1)="11" Or (Rodents.note1)="12" 
                Or (Rodents.note1)="13" Or (Rodents.note1)="14"))"""
raw_sample_data = retrieve_data(query_rats)
raw_sample_data['sampled'] = 1

#purge null plots and null periods
raw_sample_data = raw_sample_data.dropna(subset=["plot"])
raw_sample_data = raw_sample_data.dropna(subset=["period"])

#generate table showing which periods are missing plots
plot_list = raw_sample_data['plot'].unique()
period_list = raw_sample_data['period'].unique()
plot_counts = raw_sample_data.groupby('period').plot.nunique()
plot_counts = pd.DataFrame(plot_counts)
plot_counts = plot_counts.rename(columns = {'plot':'plot_count'})
plot_counts.reset_index(inplace=True)
periods_missing_plots = plot_counts[plot_counts['plot_count'] < len(plot_list)]

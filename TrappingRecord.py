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
the data was not from a normal census. It includes records tagged as empty plots"""
query_rats = """SELECT Rodents.period, Rodents.plot FROM Rodents 
                WHERE (((Rodents.period)>0 AND Rodents.period < 429) AND ((Rodents.note1) Is Null 
                Or (Rodents.note1)="1" Or (Rodents.note1)="2"
                Or (Rodents.note1)="3" Or (Rodents.note1)="6" 
                Or (Rodents.note1)="7" Or (Rodents.note1)="10" 
                Or (Rodents.note1)="11" Or (Rodents.note1)="12" 
                Or (Rodents.note1)="13" Or (Rodents.note1)="14"))"""
raw_sample_data = retrieve_data(query_rats)

#purge null plots and null periods and reduce to unique plot/period events 
# & add column denoting that these records are linked to real trapping events
raw_sample_data = raw_sample_data.dropna(subset=["plot"])
raw_sample_data = raw_sample_data.dropna(subset=["period"])
Trapping_Table = raw_sample_data.drop_duplicates()
Trapping_Table['sampled'] = 1

#generate list of periods with fewer than 24 plots recorded
period_list = Trapping_Table['period'].unique()
plot_counts = Trapping_Table.groupby('period').plot.nunique()
plot_counts = pd.DataFrame(plot_counts)
plot_counts = plot_counts.rename(columns = {'plot':'plot_count'})
plot_counts.reset_index(inplace=True)
periods_missing_plots = plot_counts[plot_counts['plot_count'] < 24]

#Find missing plots for a particular given period
plot_list = Trapping_Table['plot'].unique()
short_periods = periods_missing_plots['period'].unique()
new_data = pd.DataFrame(columns=['period', 'plot', 'sampled'])
for unique_period in short_periods:
    short_period_data = raw_sample_data[raw_sample_data['period'] == unique_period]
    short_period_plots = set(short_period_data['plot'].unique())
    missing_plots = set(plot_list).difference(short_period_plots)
    for each_plot in missing_plots:
        plot_data = pd.DataFrame([[unique_period, each_plot, 0]],
                                 columns=['period', 'plot', 'sampled'])
        new_data = new_data.append(plot_data, ignore_index=True)
Trapping_Table = Trapping_Table.append(new_data, ignore_index=True)
Trapping_Table.to_csv("Trapping_Table.csv")

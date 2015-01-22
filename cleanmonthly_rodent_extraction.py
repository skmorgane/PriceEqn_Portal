import sqlalchemy
import pandas as pd
import numpy as np
import matplotlib as plt
import json

###### FUNCTIONS

def check_for_missing_periods(data):
    Periods_unique=data.unique()
    Total_periods =data.max() - (data.min() - 1)
    
    if Total_periods != len(Periods_unique):
        outcome = "Error: Missing Periods"
    else:
        outcome = "No Missing Periods"
    return outcome

######  MAIN CODE

###### Steps for Extracting and Processing Rodent Data

# extracting the subset of raw data needed for the project from Portal Database on server
query_rats = """SELECT Rodents.mo, Rodents.dy, Rodents.yr, Rodents.period, Rodents.plot, Plots.`Type Code`, Rodents.note1, Rodents.species, Rodents.wgt 
           FROM Rodents JOIN Plots JOIN SPECIES
           ON Rodents.plot=Plots.`Plot Number`
           AND Rodents.species=SPECIES.`New Code`
           WHERE (Plots.`Type Code` != 'RE')
           AND (Rodents.period > 0)
           AND (Rodents.period < 429)
           AND (SPECIES.Rodent = 1)
           """
credentials = json.load(open("db_credentials.json", "r"))
engine = sqlalchemy.create_engine('mysql+pymysql://morgan:{}@{}:{}/{}'.format(credentials['password'], credentials['host'], credentials['port'], credentials['database']))
raw_data=pd.read_sql_query(query_rats, engine)
plot_info = pd.read_sql_table("Plots", engine)
plot_info.rename(columns={'Plot Number': 'plot'}, inplace=True)
print check_for_missing_periods(raw_data['period'])

# inserts species average mass for missing values & calculates individual energy use
raw_data['wgt'] = raw_data[['species','wgt']].groupby("species").transform(lambda x: x.fillna(x.mean()))
raw_data['Type'] = raw_data['Type Code'].map({'CO': 0, 'LK': 1, 'RE':2, 'SK':1})
raw_data['energy'] = 5.69 * raw_data['wgt'] ** 0.75

##### Steps for Calculating Treatment Average Energy Use Per Species

# importing trapping table, adding plot type info to table, calculating number of
# plots censused per period
Trapping_Table = pd.read_csv('Trapping_Table.csv')
Trapping_Table = pd.merge(Trapping_Table, plot_info, how='left', on='plot')
Trapping_Table['Type'] = Trapping_Table['Type Code'].map({'CO': 0, 'LK': 1, 'RE':2, 'SK':1})
period_plot_count = Trapping_Table[['period', 'Type', 'sampled']].groupby(['period', 'Type']).sum()
period_plot_count.reset_index(inplace=True)

# calculates mean energy use per species per plot for each trapping session
treatment_sums = raw_data[['period', 'plot', 'Type', 'species', 'energy']].groupby(['period', 'Type', 'species']).sum()
treatment_sums.reset_index(inplace=True)
treatment_sums = pd.merge(treatment_sums, period_plot_count, how='left', on=['period', 'Type'])
treatment_sums['average'] = treatment_sums['energy']/treatment_sums['sampled']

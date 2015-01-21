import sqlalchemy
import pandas as pd
import numpy as np
import matplotlib as plt
import json

def check_for_missing_periods(data):
    Periods_unique=data.unique()
    Total_periods =data.max() - (data.min() - 1)
    
    if Total_periods != len(Periods_unique):
        outcome = "Error: Missing Periods"
    else:
        outcome = "No Missing Periods"
    return outcome

#extracting just the raw data needed for the project from Portal Database on Server
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

#inserts species average mass for missing values
raw_data['wgt'] = raw_data[['species','wgt']].groupby("species").transform(lambda x: x.fillna(x.mean()))
raw_data['Type'] = np.where(raw_data['Type Code'] == 'CO', 1, 0)

#checking for missing periods
print check_for_missing_periods(raw_data['period'])

#uses individual mass to calculate an individual's energy use
raw_data['energy'] = 5.69 * raw_data['wgt'] ** 0.75

#calculates mean energy use per species per plot for each trapping session
plot_sums = raw_data[['period', 'plot', 'Type', 'species', 'energy']].groupby(['period', 'plot', 'Type', 'species']).sum()
plot_sums.reset_index(inplace=True)
treatment_avg = plot_sums[['period', 'Type', 'species', 'energy']].groupby(['period', 'Type', 'species']).mean()

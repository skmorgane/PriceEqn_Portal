import sqlalchemy
import pandas as pd
import numpy as np
import json

###### FUNCTIONS

def check_for_missing_periods(data):
    
    """ Checks for missing periods
    
    Checks that the number of periods calculated by length of unique period
    is the same as the length calculated by compariong first and last period
    number. A mismatch between these approaches indicates that there is a
    trapping period missing from the datafile. If the code continues, 
    issues will ensue
    
    Args:
        data: column from imported data table which should consist of integer
            values.
    
    Returns:
        A message about the state of the test
    """
    Periods_unique=data.unique()
    Total_periods =data.max() - (data.min() - 1)
    
    if Total_periods != len(Periods_unique):
        outcome = "Error: Missing Periods"
    else:
        outcome = "No Missing Periods"
    return outcome

######  MAIN CODE

# Extracts subset of database from server that is needed for the project
# This requires access to the json file to log on to the server.

# Query: Pulls out each rodent record that matches the following filter: Not on
#        A removal plot, captured after period 118 but before 429 (to match
#        Thibault et al 2008), is an identified rodent species.

query_rats = """SELECT Rodents.mo, Rodents.dy, Rodents.yr, 
                Rodents.period, Rodents.plot, Plots.`Type Code`, 
                Rodents.note1, Rodents.species, Rodents.wgt 
                FROM Rodents JOIN Plots JOIN SPECIES
                ON Rodents.plot=Plots.`Plot Number`
                AND Rodents.species=SPECIES.`New Code`
                WHERE (Plots.`Type Code` != 'RE') 
                AND (Rodents.period > 118) AND (Rodents.period < 429)
                AND (SPECIES.Rodent = 1) AND (SPECIES.Unknown = 0)
                """
credentials = json.load(open("db_credentials.json", "r"))
engine = sqlalchemy.create_engine('mysql+pymysql://morgan:{}@{}:{}/{}'
                                  .format(credentials['password'], 
                                          credentials['host'], 
                                          credentials['port'], 
                                          credentials['database']))
raw_data=pd.read_sql_query(query_rats, engine)
plot_info = pd.read_sql_table("Plots", engine)
plot_info.rename(columns={'Plot Number': 'plot'}, inplace=True)
print check_for_missing_periods(raw_data['period'])

# inserts species average mass for missing values, calculates 
# individual energy use, and recodes treatment codes to a numeric

raw_data['wgt'] = raw_data[['species','wgt']].groupby("species").transform(lambda x: x.fillna(x.mean()))
raw_data['Type'] = raw_data['Type Code'].map({'CO': 0, 'LK': 1, 
                                              'RE':2, 'SK':1})
raw_data['energy'] = 5.69 * raw_data['wgt'] ** 0.75

# imports trapping table, adds treatment type info to trapping table, calculates 
# number of plots censused per period

Trapping_Table = pd.read_csv('Trapping_Table.csv')
Trapping_Table = pd.merge(Trapping_Table, plot_info, how='left', 
                          on='plot')
Trapping_Table['Type'] = Trapping_Table['Type Code'].map({'CO': 0, 
                                                          'LK': 1, 
                                                          'RE':2, 
                                                          'SK':1})
period_plot_count = Trapping_Table[['period', 'Type', 'sampled']].groupby(['period', 'Type']).sum()
period_plot_count.reset_index(inplace=True)

# calculates mean energy use per species per treatment for each trapping 
# session using actual number of plots sampled as determined from the trapping
# table.  Because sometimes plots have no rodents you cannot just average by
# the number of unique plots in the database

treatment_sums = raw_data[['period', 'plot', 'Type', 'species', 'energy'
                           ]].groupby(['period', 'Type', 'species']).sum()
treatment_sums.reset_index(inplace=True)
treatment_sums = pd.merge(treatment_sums, period_plot_count, how='left',
                          on=['period', 'Type'])
treatment_sums['average'] = treatment_sums['energy']/treatment_sums['sampled']

# determines first Julian Date of trapping for each period, processes and merges
# with data for export

JulianDate_for_period = Trapping_Table[['JulianDate', 
                                        'period']].groupby(['period']).min()
JulianDate_for_period['JulianDate'] = JulianDate_for_period['JulianDate'].astype(int)
JulianDate_for_period.reset_index(inplace=True)
treatment_sums = pd.merge(treatment_sums, JulianDate_for_period, how='left',
                          on=['period'])

# formatting and output for analysis

treatment_data_export = treatment_sums.drop(['plot', 'energy', 'sampled'], 
                                            axis = 1)                                             
treatment_data_export.to_csv("Portal_Rodents_PriceProject.csv")

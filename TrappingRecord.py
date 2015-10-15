import sqlalchemy
import pandas as pd
import numpy as np
import json
import jdcal

###### FUNCTIONS

def retrieve_data(query):
    
    """Execute SQL Query on Portal Rodent Database on Serenity 
       
    Executes a mySQL query and returns the data in a Pandas dataframe. This 
    function is currently constructed to work for a specific user and 
    only if the JSON file "db_credentials" for accessing the database is
    in the same folder

    Args:
        query: a doc string containing the sql query being sent to the sql 
        database
    
    Returns:
        data: the data extracted using the query in a pandas dataframe format
    """
    
    credentials = json.load(open("db_credentials.json", "r"))
    engine = sqlalchemy.create_engine('mysql+pymysql://morgan:{}@{}:{}/{}'.format(credentials['password'], credentials['host'], credentials['port'], credentials['database']))
    data=pd.read_sql_query(query, engine)
    return data

def convert_to_JulianDate(row):
    
    """Creates Julian Dates
    
    Takes each row of the Trapping_Table dataframe and applies the 
    Julian Date Converter to the Gregorian Date
    
    Args:
        row: a row from a pandas dataframe containing a separate columns for
            year, month, day
    
    Returns:
        a single numeric value representing the Julian Date of the year, month,
        day given to the function
        """
    
    return sum(jdcal.gcal2jd(int(row['yr']), int(row['mo']), 
                             int(row['dy']))) 

###### MAIN CODE

# Load Data and and Add 1 to denote that an existing record represents a 
# trapping event. Query excludes negative periods and records with notes 
# that indicate the data was not from a normal census. It includes 
# records tagged as empty plots

query_rats = """SELECT Rodents.yr, Rodents.mo, Rodents.dy, 
                Rodents.period, Rodents.plot FROM Rodents 
                WHERE ((Rodents.period)>0) 
                AND (Rodents.note1 Is Null 
                OR Rodents.note1 IN ("1", "2", "3", "6", "7", "10", 
               "11", "12", "13", "14"))
               """
raw_sample_data = retrieve_data(query_rats)

# Reduce to unique plot/period events & add column denoting that
# records are linked to real trapping events

raw_sample_data = raw_sample_data.dropna(subset=["plot"])
raw_sample_data = raw_sample_data.dropna(subset=["period"])
Trapping_Table = raw_sample_data.drop_duplicates()
Trapping_Table['sampled'] = 1

# Generate list of periods with fewer than 24 plots recorded

period_list = Trapping_Table['period'].unique()
plot_counts = Trapping_Table.groupby('period').plot.nunique()
plot_counts = pd.DataFrame(plot_counts)
plot_counts = plot_counts.rename(columns = {'plot':'plot_count'})
plot_counts.reset_index(inplace=True)
periods_missing_plots = plot_counts[plot_counts['plot_count'] < 24]

# Find missing plots for a particular given period & collect information on them

plot_list = Trapping_Table['plot'].unique()
short_periods = periods_missing_plots['period'].unique()
new_data = pd.DataFrame(columns=['period', 'plot', 'sampled'])
for unique_period in short_periods:
    short_period_data = raw_sample_data[raw_sample_data['period'] == 
                                        unique_period]
    short_period_plots = set(short_period_data['plot'].unique())
    missing_plots = set(plot_list).difference(short_period_plots)
    year = int(short_period_data['yr'].min())   #first year of trapping period
    month = int(short_period_data['mo'].min())  #first month of trapping period
    day = int(short_period_data['dy'].min())    #first day of trapping period
    for each_plot in missing_plots:
        plot_data = pd.DataFrame([[year, month, day, unique_period, each_plot, 
                                   0]], columns=['yr', 'mo', 'dy','period', 
                                                 'plot', 'sampled'])
        new_data = new_data.append(plot_data, ignore_index=True)

# Add information about missed plots & Julian Date to the trapping table

Trapping_Table = Trapping_Table.append(new_data, ignore_index=True)
Trapping_Table['JulianDate'] = Trapping_Table.apply(convert_to_JulianDate, axis=1)

# Export

Trapping_Table.to_csv("Trapping_Table.csv")

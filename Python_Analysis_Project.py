#!/usr/bin/env python
# coding: utf-8

# ## Project Title:
# ### COVID-19 Outcomes by Testing Cohorts: Cases, Hospitalizations, and Deaths

# ### Problem Statement
# 
# - As we tend to observe about the new variants(JN1 which is a recent variant i.e, DEC. 14, 2023) of covid here and there, the primary goal is to derive meaningful insights and actionable information to support public health decision-making and crisis management. 

# ### Data Description
# 
# - The data is taken from: https://data.cityofnewyork.us/Health/COVID-19-Outcomes-by-Testing-Cohorts-Cases-Hospita/cwmx-mvra
# 
# - It contains 176k rows and 6 columns

# ### Columns in this Dataset
# 
# 1. extract_date : Date of extraction from live disease surveillance database.
# 2. specimen_date : Date of specimen collection, equivalent to diagnosis date.
# 3. Number_tested : Count of NYC residents newly tested for SARS-CoV-2.
# 4. Number_confirmed : Count of patients tested who were confirmed to be COVID-19 cases.
# 5. Number_hospitalized : Count of confirmed COVID-19 cases among patients ever hospitalized.
# 6. Number_deaths : Count of confirmed COVID-19 cases among patients who died.

# In[8]:


import sqlite3
from sqlite3 import Error
file_path = "/Users/arjun/Downloads/COVID-19_Outcomes_by_Testing_Cohorts__Cases__Hospitalizations__and_Deaths_20231217.csv"


# #### For creating a connection to an SQLite database, and the optional delete_db parameter allows for deleting the existing database file before creating a new one.

# In[9]:


def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


# #### The purpose of this function is to create a table in the SQLite database specified by the connection object

# In[10]:


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


# #### Establish a connection to an SQLite database file named "test1.db".
# #### create cursor for executing SQL queries like inserting, deleting and fetching results from the database.

# In[11]:


create_normalized    = create_connection("test1.db")
cursor_normalized    = create_normalized.cursor()


# #### This code defines SQL statements to create three tables named DATEINFO, TESTINFO, and CASESINFO. These tables are designed to store information related to COVID-19 testing, including dates, test information, and case outcomes. These tables are designed with a relational structure, using foreign key relationships to link records across the DATEINFO, TESTINFO, and CASESINFO tables. This structure is common in relational database design, allowing for efficient data organization and retrieval.

# In[12]:


create_date_table = """CREATE TABLE IF NOT EXISTS DATEINFO (
TEST_ID INTEGER PRIMARY KEY AUTOINCREMENT,
EXTRACT_DATE DATE,
SPECIMEN_DATE DATE); """

create_test_table = """CREATE TABLE IF NOT EXISTS TESTINFO (
TEST_ID INTEGER PRIMARY KEY AUTOINCREMENT,
NUMBER_TESTED INTEGER,
FOREIGN KEY (TEST_ID) REFERENCES DATEINFO(TEST_ID)
); """

create_cases_table = """CREATE TABLE IF NOT EXISTS CASESINFO (
TEST_ID INTEGER PRIMARY KEY AUTOINCREMENT,
NUMBER_CONFIRMED INTEGER,
NUMBER_HOSPITALIZED INTEGER,
NUMBER_DEATHS INTEGER,
FOREIGN KEY (TEST_ID) REFERENCES DATEINFO(TEST_ID)
);"""


# #### This code reads data from a CSV file and performs data parsing and data processing 

# In[13]:


with open(file_path, 'r') as file:
    csv_reader = file.readlines()
    header = csv_reader[0].strip().split(',')
    rows = csv_reader[1:]
    parsed_row = lambda row : dict(zip(header, row.strip().split(',')))
    parsed_data = [parsed_row(row) for row in rows]
    unique_data = [dict(t) for t in set(tuple(d.items()) for d in parsed_data)]
    print(unique_data[0:5])


# #### Here we're calling the create_table function to create the DATAINFO table and insert the parsed data into the table using cursor

# In[14]:


create_table(create_normalized, create_date_table)
date_list = [(row['extract_date'], row['specimen_date']) for row in unique_data]

# Assuming conn_non_normalized is the connection object and cursor_non_normalized is the cursor object
cursor_normalized.executemany("""
    INSERT INTO DATEINFO 
    ([EXTRACT_DATE], [SPECIMEN_DATE]) 
    VALUES (?,?);
""", date_list)
create_normalized.commit()
cursor_normalized.execute("SELECT * FROM DATEINFO")
data = cursor_normalized.fetchall()
print(data[0:5])


# #### Here we're calling the create_table function to create the TESTINFO table and insert the parsed data into the table using cursor

# In[15]:


create_table(create_normalized, create_test_table)
test_list = [(row['Number_tested']) for row in unique_data]
test_list= [[i] for i in test_list]
cursor_normalized.executemany("""
    INSERT INTO TESTINFO 
    (NUMBER_TESTED) 
    VALUES (?);
    """,test_list)


# In[16]:


cursor_normalized.execute("SELECT * FROM TESTINFO")
data = cursor_normalized.fetchall()
print(data[0:5])


# #### Here we're calling the create_table function to create the CASESINFO table and insert the parsed data into the table using cursor

# In[17]:


create_table(create_normalized, create_cases_table)
cases_list = [(row['Number_confirmed'], row['Number_hospitalized'], row['Number_deaths']) for row in unique_data]
cursor_normalized.executemany("""
   INSERT INTO CASESINFO 
   ([NUMBER_CONFIRMED], [NUMBER_HOSPITALIZED], [NUMBER_DEATHS]) 
   VALUES (?,?,?);
""", cases_list)


# In[18]:


cursor_normalized.execute("SELECT * FROM CASESINFO")
data = cursor_normalized.fetchall()
print(data[0:5])


# #### Executed a SQL query using joins and stored in the Pandas DataFrame df
# #### Each row in the DataFrame corresponds to a record retrieved from the SQL query.
# #### The columns in the DataFrame match the selected columns from the SQL query.
# 
# #### This dataset preserves historical records and source data changes, so each extract date reflects the current copy of the data as of that date. For example, an extract date of 5/1/2020 and extract date of 5/2/2020 will both contain all records as they were as of that extract date.

# In[19]:


import pandas as pd
df = pd.read_sql_query("""
    SELECT D.TEST_ID, D.EXTRACT_DATE, D.SPECIMEN_DATE, T.NUMBER_TESTED, C.NUMBER_CONFIRMED, C.NUMBER_HOSPITALIZED, C.NUMBER_DEATHS
    FROM DATEINFO D
    JOIN TESTINFO T ON D.TEST_ID = T.TEST_ID
    JOIN CASESINFO C ON D.TEST_ID = C.TEST_ID
    GROUP BY D.EXTRACT_DATE
    ORDER BY D.TEST_ID ASC
""", create_normalized)


# #### Use df.head() which is a Pandas method used to display the first few rows of a DataFrame

# In[20]:


df.head()


# #### Use df.tail() which is a Pandas method used to display the last few rows of a DataFrame

# In[21]:


df.tail()


# In[22]:


df.shape


# #### Checking for null values and missing values in the dataframe

# In[25]:


df.isnull().sum()


# In[26]:


df.isna().sum()


# #### Tryng to fill the missing values with NAN but in our case we don't have any missing values

# In[27]:


df.fillna(value='NAN', method=None, axis=None, inplace=False, limit=None, downcast=None)


# In[28]:


df['EXTRACT_DATE'] = pd.to_datetime(df['EXTRACT_DATE'], errors='coerce')
df['SPECIMEN_DATE'] = pd.to_datetime(df['SPECIMEN_DATE'], errors='coerce')


# In[29]:


df = df.dropna(subset=['EXTRACT_DATE'])
df = df.dropna(subset=['SPECIMEN_DATE'])
df.tail()


# In[30]:


df['EXTRACT_MONTH'] = df['EXTRACT_DATE'].dt.month
df['EXTRACT_YEAR'] = df['EXTRACT_DATE'].dt.year


# In[31]:


df['SPECIMEN_MONTH'] = df['SPECIMEN_DATE'].dt.month
df['SPECIMEN_YEAR'] = df['SPECIMEN_DATE'].dt.year
df


# In[32]:


import matplotlib.pyplot as plt
import seaborn as sns
import ipywidgets as widgets
from IPython.display import display


# In[33]:


def analyze_data(analysis_type, month, year):
    filtered_df = df[(df['EXTRACT_MONTH'] == month) & (df['EXTRACT_YEAR'] == year)]
    if analysis_type == 'Plot Test Results':
        sns.lineplot(x=filtered_df['EXTRACT_DATE'].dt.day, y='NUMBER_TESTED', data=filtered_df)
        plt.title('Test Results Over Time')
        plt.show()
    elif analysis_type == 'Plot Confirmed Cases':
        sns.barplot(x=filtered_df['EXTRACT_DATE'].dt.day, y='NUMBER_CONFIRMED', data=filtered_df, errorbar=None)
        plt.title('Confirmed Cases Over Time')
        plt.show()
    elif analysis_type == 'Plot Hospitalized Cases':
        sns.barplot(x=filtered_df['EXTRACT_DATE'].dt.day, y='NUMBER_HOSPITALIZED', data=filtered_df, errorbar=None)
        plt.title('Hospitalized Cases Over Time')
        plt.show()
    elif analysis_type == 'Plot Deaths':
        sns.barplot(x=filtered_df['EXTRACT_DATE'].dt.day, y='NUMBER_DEATHS', data=filtered_df, errorbar=None)
        plt.title('Deaths Over Time')
        plt.show()
    elif analysis_type == 'Summary Statistics':
        summary_stats = filtered_df.describe()
        display(summary_stats)


# In[34]:


month_dropdown = widgets.Dropdown(
    options=df['EXTRACT_MONTH'].unique(),
    description='Month:'
)

year_dropdown = widgets.Dropdown(
    options=df['EXTRACT_YEAR'].unique(),
    description='Year:'
)


# In[35]:


analysis_buttons = widgets.ToggleButtons(
    options=['Plot Test Results', 'Plot Confirmed Cases', 'Plot Hospitalized Cases', 'Plot Deaths', 'Summary Statistics'],
    description='Select Analysis:'
)


# In[36]:


widgets.interactive(analyze_data, analysis_type=analysis_buttons, month=month_dropdown, year=year_dropdown)


# - Line Plot (Test Results Over Time):
#     Look for trends in the number of tests conducted over time.
#     Identify periods of increase or decrease in testing activity.
#     Note any spikes or drops in the number of tests, which might indicate specific events or changes.
# 
# - Bar Plot (Confirmed Cases Over Time):
#     Examine the daily variations in the number of confirmed cases, no.of deaths and no. of people hospitalized
#     Identify any sudden increases in confirmed cases, which might indicate outbreaks or changes in testing strategies.
#     Look for patterns in the data that might reveal insights into the spread of the virus.
# 
# - Summary Statistics:
#     Examine key statistics such as mean, median, minimum, maximum, and quartiles.
#     Understand the central tendency and variability in the data.

# In[37]:


def analyze_data(analysis_type, start_date, end_date):
    # Convert date strings to datetime objects
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    # Filter data for the selected date range
    filtered_df = df[(df['EXTRACT_DATE'] >= start_date) & (df['EXTRACT_DATE'] <= end_date)]
    
    if analysis_type == 'Pie Chart':
        plt.pie(filtered_df[['NUMBER_CONFIRMED', 'NUMBER_HOSPITALIZED', 'NUMBER_DEATHS']].sum(),
                labels=['Confirmed', 'Hospitalized', 'Deaths'],
                autopct='%1.1f%%', startangle=140)
        plt.title('Distribution of Cases')
        plt.show()
    elif analysis_type == 'Box Plot':
        sns.boxplot(data=filtered_df[['NUMBER_CONFIRMED', 'NUMBER_HOSPITALIZED', 'NUMBER_DEATHS']])
        plt.title('Box Plot of Cases')
        plt.show()


# In[38]:


start_date_picker = widgets.DatePicker(
    description='Start Date:',
    value=pd.to_datetime('2021-01-01')
)

end_date_picker = widgets.DatePicker(
    description='End Date:',
    value=pd.to_datetime('2021-01-05')
)


# In[39]:


analysis_buttons = widgets.ToggleButtons(
    options=['Pie Chart', 'Box Plot'],
    description='Select Analysis:'
)


# In[40]:


widgets.interactive(analyze_data, analysis_type=analysis_buttons, start_date=start_date_picker, end_date=end_date_picker)


# - Pie Chart (Distribution of Cases):
#     Understand the proportion of confirmed cases, hospitalized cases, and deaths.
#     Identify the percentage of cases in each category relative to the total.
#     
# - Box Plot (Box Plot of Cases):
#     Assess the distribution of cases by examining the box plot.
#     Identify the median, quartiles, and any outliers.
#     Understand the spread and variability in the number of confirmed, hospitalized, and death cases.
#     

# In[ ]:





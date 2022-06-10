#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[1]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[4]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[5]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[6]:


try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace

# In[7]:


try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# In[8]:


# Creates new table to stored data for the query
query = "CREATE TABLE IF NOT EXISTS artis_song "
query = query + "(session_id int, item_session int, artist_name text, song_title text, song_length float, PRIMARY KEY (session_id, item_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)
                    


# In[9]:


# Set up the CSV file.
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO artis_song (session_id, item_session, artist_name, song_title, song_length)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))


# #### Do a SELECT to verify that the data have been inserted into each table

# In[10]:


## Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4

query = "SELECT artist_name, song_title, song_length FROM artis_song WHERE session_id=338 AND item_session=4"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist_name, row.song_title, row.song_length)


# ### Create new user_artist table

# In[11]:


# Creates user_artist table with data that can be use for the query
query = "CREATE TABLE IF NOT EXISTS user_artist "
query = query + "(                    user_id int,                     item_session int,                     session_id int,                     artist_name text,                     first_name text,                     last_name text,                     song_title text,                     PRIMARY KEY ((user_id, session_id), item_session)                )"
try:
    session.execute(query)
except Exception as e:
    print(e)
                 


# ### Insert statements into new table

# In[ ]:


# We have provided part of the code to set up the CSV file.
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## Assign the INSERT statements into the `query` variable
        query = "INSERT INTO user_artist (        user_id, item_session, session_id,         artist_name, first_name, last_name,         song_title)"
        query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (int(line[10]), int(line[3]), int(line[8]), line[0], line[1], line[4], line[9]))


# ### Query 2

# In[ ]:


## Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
query = "SELECT artist_name, song_title, first_name, last_name FROM user_artist WHERE user_id=10 AND session_id=182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist_name, row.song_title, row.first_name, row.last_name)
    


# ### Create new user_artist table for Query 3

# In[19]:


# Creates user_artist table with data that can be use for the query
query = "CREATE TABLE IF NOT EXISTS user_song "
query = query + "(                    first_name text,                     last_name text,                     song_title text,                     PRIMARY KEY (song_title)                )"
try:
    session.execute(query)
except Exception as e:
    print(e)


# ### Insert statements into new table

# In[21]:


# We have provided part of the code to set up the CSV file.
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## Assign the INSERT statements into the `query` variable
        query = "INSERT INTO user_song (first_name, last_name, song_title)"
        query = query + " VALUES (%s, %s, %s)"
        ## TAssign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (line[1], line[4], line[9]))
    


# ### Query 3

# In[22]:


## Give me every user name (first and last) in my music app history 
# who listened to the song 'All Hands Against His Own'
song = 'All Hands Against His Own'
query = "SELECT song_title, first_name, last_name FROM user_song WHERE song_title='" + song + "'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.first_name, row.last_name)
   
                    


# In[ ]:





# ### Drop the tables before closing out the sessions

# In[23]:


## Drop the table before closing out the sessions
query = "drop table artis_song"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# In[25]:


## Drop the table before closing out the sessions
query = "drop table user_song"
try:
    rows = session.execute(query)
except Exception as e:
    print(e


# In[ ]:


## Drop the table before closing out the sessions
query = "drop table user_artist"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

# In[ ]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:





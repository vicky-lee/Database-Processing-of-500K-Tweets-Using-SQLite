# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 22:57:50 2015

@author: Vicky Lee
"""
#Final - Vicky Lee

#Problem 1A
import sqlite3
import json
import time

# Create UserTable, GeoTable, and TweetTable
UserTable = '''CREATE TABLE User
(
  user_id NUMBER,
  name VARCHAR(40),
  screen_name VARCHAR(40),
  description VARCHAR(140),
  friends_count NUMBER,
  
  CONSTRAINT User_PK
     PRIMARY KEY(user_id)
     
);'''

GeoTable = '''CREATE TABLE Geo
(
  geo_id VARCHAR(50),
  type VARCHAR(20),
  latitude NUMBER,
  longitude NUMBER,
  
  CONSTRAINT Geo_PK
     PRIMARY KEY(geo_id)
     
);'''

TweetTable = '''CREATE TABLE Tweet
(
  id_str NUMBER(40),
  created_at DATE,
  text VARCHAR(200),
  source VARCHAR(200) DEFAULT NULL,
  in_reply_to_user_id NUMBER(20),
  in_reply_to_screen_name VARCHAR(60),
  in_reply_to_status_id NUMBER(20),
  contributors VARCHAR(200),
  retweet_count NUMBER(10),
  user_id NUMBER,
  geo_id VARCHAR(50),
  
  CONSTRAINT Tweet_PK
     PRIMARY KEY(id_str)
     
  CONSTRAINT Tweet_FK
      FOREIGN KEY(user_id)
          REFERENCES User(user_id)
          
  CONSTRAINT Tweet_FK2
      FOREIGN KEY(geo_id)
          REFERENCES Geo(geo_id)
          
);'''


conn = sqlite3.connect('Tweet_Database.db')

c = conn.cursor()

c.execute("DROP TABLE IF EXISTS User")
c.execute("DROP TABLE IF EXISTS Geo")
c.execute("DROP TABLE IF EXISTS Tweet")

c.execute(UserTable)
c.execute(GeoTable)
c.execute(TweetTable)


#Problem1B
#Download 500k tweets and time it
#It took 3647 seconds to download 500K tweets from the web and 
#to save to a local text file.

import urllib.request as urllib
import json
import time

start = time.time()

response = urllib.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")

f = open('Tweets.txt','w',encoding="utf-8")

for i in range(500000):
    f.write(response.readline().decode("utf8"))

f.close()

end   = time.time()

print ("loadTweets took ", (end-start), ' seconds.')

#loadTweets took  3646.6679005622864  seconds.


#Problem 1C
#Instead of saving tweets to the file, populate the 3-table schema 
#It took 1329 seconds to populate the 3-table schema from the web. 
#The row counts for each of the three tables are as below:
#Table Tweet Loaded  (499776,)  rows
#Table User Loaded  (447304,)  rows
#Table Geo Loaded  (11849,)  rows

import sqlite3
import json
import time

UserTable = '''CREATE TABLE User
(
  user_id NUMBER,
  name VARCHAR(40),
  screen_name VARCHAR(40),
  description VARCHAR(140),
  friends_count NUMBER,
  
  CONSTRAINT User_PK
     PRIMARY KEY(user_id)
     
);'''

GeoTable = '''CREATE TABLE Geo
(
  geo_id VARCHAR(50),
  type VARCHAR(20),
  latitude NUMBER,
  longitude NUMBER,
  
  CONSTRAINT Geo_PK
     PRIMARY KEY(geo_id)
     
);'''

TweetTable = '''CREATE TABLE Tweet
(
  id_str NUMBER(40),
  created_at DATE,
  text VARCHAR(200),
  source VARCHAR(200) DEFAULT NULL,
  in_reply_to_user_id NUMBER(20),
  in_reply_to_screen_name VARCHAR(60),
  in_reply_to_status_id NUMBER(20),
  contributors VARCHAR(200),
  retweet_count NUMBER(10),
  user_id NUMBER,
  geo_id VARCHAR(50),
  
  CONSTRAINT Tweet_PK
     PRIMARY KEY(id_str)
     
  CONSTRAINT Tweet_FK
      FOREIGN KEY(user_id)
          REFERENCES User(user_id)
          
  CONSTRAINT Tweet_FK2
      FOREIGN KEY(geo_id)
          REFERENCES Geo(geo_id)
     
);'''


conn = sqlite3.connect('Tweet_Database.db')


c = conn.cursor()

c.execute("DROP TABLE IF EXISTS User")
c.execute("DROP TABLE IF EXISTS Geo")
c.execute("DROP TABLE IF EXISTS Tweet")

c.execute(UserTable)
c.execute(GeoTable)
c.execute(TweetTable)


import urllib.request as urllib
import json
import time

def loadTweets(tweetLines):

    batchRows = 1
    batchedInsertsUser = []
    batchedInsertsGeo = []
    batchedInsertsTweet = []

    while len(tweetLines) > 0:
        line = tweetLines.pop(0) 
    
        jsonobject = json.loads(line)

        newRowUser = [] 
        newRowGeo = [] 
        newRowTweet = [] 
        
        UserValues = [jsonobject['user']['id'],jsonobject['user']['name'],jsonobject['user']['screen_name'],
                    jsonobject['user']['description'],jsonobject['user']['friends_count']]       
        
        for value in UserValues:
            if value in ['',[],'null']:
                newRowUser.append(None)
            else:
                newRowUser.append(value)
                
        batchedInsertsUser.append(newRowUser)
        

        if jsonobject['geo'] != None:
            GeoValues = [str(jsonobject['geo']['coordinates'][0])+'&'+str(jsonobject['geo']['coordinates'][1]),jsonobject['geo']['type'],jsonobject['geo']['coordinates'][0],jsonobject['geo']['coordinates'][1]] 
           
            for value in GeoValues:
                if value in ['',[],'null']:
                    newRowGeo.append(None)
                else:
                    newRowGeo.append(value)
            
            batchedInsertsGeo.append(newRowGeo)        
        
        
        TweetKeys = ['id_str','created_at','text','source','in_reply_to_user_id', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'contributors']

        for key in TweetKeys:

            if jsonobject[key] in ['',[],'null']:
                newRowTweet.append(None)
            else:
                newRowTweet.append(jsonobject[key])

        if 'retweeted_status' in jsonobject.keys():
            newRowTweet.append(jsonobject['retweeted_status']['retweet_count'])
        else:
            if jsonobject['retweet_count'] in ['',[],'null']:
                newRowTweet.append(None)
            else:
                newRowTweet.append(jsonobject['retweet_count'])
                   

        if jsonobject['user']['id'] in ['',[],'null']:
            newRowTweet.append(None)
        else:
            newRowTweet.append(jsonobject['user']['id'])
        

        if jsonobject['geo'] != None:
            newRowTweet.append(str(jsonobject['geo']['coordinates'][0])+'&'+str(jsonobject['geo']['coordinates'][1]))
        else:
            newRowTweet.append(None)
            
        batchedInsertsTweet.append(newRowTweet)
        

        if len(batchedInsertsUser) >= batchRows or len(tweetLines) == 0:
            c.executemany('INSERT OR REPLACE INTO User VALUES(?,?,?,?,?)', batchedInsertsUser)
            batchedInsertsUser = []
        if len(batchedInsertsGeo) >= batchRows or len(tweetLines) == 0:
            c.executemany('INSERT OR REPLACE INTO Geo VALUES(?,?,?,?)', batchedInsertsGeo)
            batchedInsertsGeo = []
        if len(batchedInsertsTweet) >= batchRows or len(tweetLines) == 0:
            c.executemany('INSERT OR REPLACE INTO Tweet VALUES(?,?,?,?,?,?,?,?,?,?,?)', batchedInsertsTweet)
            batchedInsertsTweet = []

start = time.time()
response = urllib.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")

lines = []
for i in range(500000):
    lines.append(response.readline().decode("utf8"))


loadTweets(lines)

end   = time.time()

print ("loadTweets took ", (end-start), ' seconds.')
print ("Table Tweet Loaded ", c.execute('SELECT COUNT(*) FROM Tweet').fetchall()[0], " rows")
print ("Table User Loaded ", c.execute('SELECT COUNT(*) FROM User').fetchall()[0], " rows")
print ("Table Geo Loaded ", c.execute('SELECT COUNT(*) FROM Geo').fetchall()[0], " rows")

c.close()
conn.commit()
conn.close()


#loadTweets took  1329.2389063835144  seconds.
#Table Tweet Loaded  (499776,)  rows
#Table User Loaded  (447304,)  rows
#Table Geo Loaded  (11849,)  rows

#Problem1D
# Use locally saved tweet file (created in part-b) to repeat the database population step 
#It took 131 seconds to load 500K tweets into the 3-table database using saved file. 
#This is much faster than loading tweets from the web which took 1329 seconds.
#The row counts for each of the three tables are as below:
#Table Tweet Loaded  (499776,)  rows
#Table User Loaded  (447304,)  rows
#Table Geo Loaded  (11849,)  rows

import sqlite3
import json
import time

UserTable = '''CREATE TABLE User
(
  user_id NUMBER,
  name VARCHAR(40),
  screen_name VARCHAR(40),
  description VARCHAR(140),
  friends_count NUMBER,
  
  CONSTRAINT User_PK
     PRIMARY KEY(user_id)
     
);'''

GeoTable = '''CREATE TABLE Geo
(
  geo_id VARCHAR(50),
  type VARCHAR(20),
  latitude NUMBER,
  longitude NUMBER,

  
  CONSTRAINT Geo_PK
     PRIMARY KEY(geo_id)
     
);'''

TweetTable = '''CREATE TABLE Tweet
(
  id_str NUMBER(40),
  created_at DATE,
  text VARCHAR(200),
  source VARCHAR(200) DEFAULT NULL,
  in_reply_to_user_id NUMBER(20),
  in_reply_to_screen_name VARCHAR(60),
  in_reply_to_status_id NUMBER(20),
  contributors VARCHAR(200),
  retweet_count NUMBER(10),
  user_id NUMBER,
  geo_id VARCHAR(50),

  CONSTRAINT Tweet_PK
     PRIMARY KEY(id_str)
     
  CONSTRAINT Tweet_FK
      FOREIGN KEY(user_id)
          REFERENCES User(user_id)
          
  CONSTRAINT Tweet_FK2
      FOREIGN KEY(geo_id)
          REFERENCES Geo(geo_id)
     
);'''


conn = sqlite3.connect('Tweet_Database.db')


c = conn.cursor()

c.execute("DROP TABLE IF EXISTS User")
c.execute("DROP TABLE IF EXISTS Geo")
c.execute("DROP TABLE IF EXISTS Tweet")

c.execute(UserTable)
c.execute(GeoTable)
c.execute(TweetTable)


import json
import time

def loadTweets(tweetLines):

    batchRows = 1
    batchedInsertsUser = []
    batchedInsertsGeo = []
    batchedInsertsTweet = []

    while len(tweetLines) > 0:
        line = tweetLines.pop(0) 
    
        jsonobject = json.loads(line)

        newRowUser = [] 
        newRowGeo = [] 
        newRowTweet = [] 
        
        UserValues = [jsonobject['user']['id'],jsonobject['user']['name'],jsonobject['user']['screen_name'],
                    jsonobject['user']['description'],jsonobject['user']['friends_count']]       
        
        for value in UserValues:
            if value in ['',[],'null']:
                newRowUser.append(None)
            else:
                newRowUser.append(value)
                
        batchedInsertsUser.append(newRowUser)
        

        if jsonobject['geo'] != None:
            GeoValues = [str(jsonobject['geo']['coordinates'][0])+'&'+str(jsonobject['geo']['coordinates'][1]),jsonobject['geo']['type'],jsonobject['geo']['coordinates'][0],jsonobject['geo']['coordinates'][1]] 
           
            for value in GeoValues:
                if value in ['',[],'null']:
                    newRowGeo.append(None)
                else:
                    newRowGeo.append(value)
            
            batchedInsertsGeo.append(newRowGeo)        
        
        
        TweetKeys = ['id_str','created_at','text','source','in_reply_to_user_id', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'contributors']

        for key in TweetKeys:

            if jsonobject[key] in ['',[],'null']:
                newRowTweet.append(None)
            else:
                newRowTweet.append(jsonobject[key])

        if 'retweeted_status' in jsonobject.keys():
            newRowTweet.append(jsonobject['retweeted_status']['retweet_count'])
        else:
            if jsonobject['retweet_count'] in ['',[],'null']:
                newRowTweet.append(None)
            else:
                newRowTweet.append(jsonobject['retweet_count'])
        

        if jsonobject['user']['id'] in ['',[],'null']:
            newRowTweet.append(None)
        else:
            newRowTweet.append(jsonobject['user']['id'])
        

        if jsonobject['geo'] != None:
            newRowTweet.append(str(jsonobject['geo']['coordinates'][0])+'&'+str(jsonobject['geo']['coordinates'][1]))
        else:
            newRowTweet.append(None)
            
        batchedInsertsTweet.append(newRowTweet)
        

        if len(batchedInsertsUser) >= batchRows or len(tweetLines) == 0:
            c.executemany('INSERT OR REPLACE INTO User VALUES(?,?,?,?,?)', batchedInsertsUser)
            batchedInsertsUser = []
        if len(batchedInsertsGeo) >= batchRows or len(tweetLines) == 0:
            c.executemany('INSERT OR REPLACE INTO Geo VALUES(?,?,?,?)', batchedInsertsGeo)
            batchedInsertsGeo = []
        if len(batchedInsertsTweet) >= batchRows or len(tweetLines) == 0:
            c.executemany('INSERT OR REPLACE INTO Tweet VALUES(?,?,?,?,?,?,?,?,?,?,?)', batchedInsertsTweet)
            batchedInsertsTweet = []


start = time.time()

fd = open('Tweets.txt', 'r', encoding='utf8')

allLines = fd.readlines()

fd.close()

loadTweets(allLines)

end   = time.time()

print ("loadTweets took ", (end-start), ' seconds.')
print ("Table Tweet Loaded ", c.execute('SELECT COUNT(*) FROM Tweet').fetchall()[0], " rows")
print ("Table User Loaded ", c.execute('SELECT COUNT(*) FROM User').fetchall()[0], " rows")
print ("Table Geo Loaded ", c.execute('SELECT COUNT(*) FROM Geo').fetchall()[0], " rows")

c.close()
conn.commit()
conn.close()

#loadTweets took  130.95757389068604  seconds.
#Table Tweet Loaded  (499776,)  rows
#Table User Loaded  (447304,)  rows
#Table Geo Loaded  (11849,)  rows

#Problem1E
#Re-run the previous step with batching size of 500 
#It took 108 seconds to load 500K tweets with batching size of 500 to populate 
#the 3-table schema from the saved text file. The runtime is faster when batching 
#is used. It took 131 seconds when batching size was 1.
#The row counts for each of the three tables are as below:
#Table Tweet Loaded  (499776,)  rows
#Table User Loaded  (447304,)  rows
#Table Geo Loaded  (11849,)  rows

import sqlite3
import json
import time

UserTable = '''CREATE TABLE User
(
  user_id NUMBER,
  name VARCHAR(40),
  screen_name VARCHAR(40),
  description VARCHAR(140),
  friends_count NUMBER,
  
  CONSTRAINT User_PK
     PRIMARY KEY(user_id)
     
);'''

GeoTable = '''CREATE TABLE Geo
(
  geo_id VARCHAR(50),
  type VARCHAR(20),
  latitude NUMBER,
  longitude NUMBER,

  
  CONSTRAINT Geo_PK
     PRIMARY KEY(geo_id)
     
);'''

TweetTable = '''CREATE TABLE Tweet
(
  id_str NUMBER(40),
  created_at DATE,
  text VARCHAR(200),
  source VARCHAR(200) DEFAULT NULL,
  in_reply_to_user_id NUMBER(20),
  in_reply_to_screen_name VARCHAR(60),
  in_reply_to_status_id NUMBER(20),
  contributors VARCHAR(200),
  retweet_count NUMBER(10),
  user_id NUMBER,
  geo_id VARCHAR(50),

  
  CONSTRAINT Tweet_PK
     PRIMARY KEY(id_str)
     
  CONSTRAINT Tweet_FK
      FOREIGN KEY(user_id)
          REFERENCES User(user_id)
          
  CONSTRAINT Tweet_FK2
      FOREIGN KEY(geo_id)
          REFERENCES Geo(geo_id)
     

     
);'''


conn = sqlite3.connect('Tweet_Database.db')


c = conn.cursor()

c.execute("DROP TABLE IF EXISTS User")
c.execute("DROP TABLE IF EXISTS Geo")
c.execute("DROP TABLE IF EXISTS Tweet")

c.execute(UserTable)
c.execute(GeoTable)
c.execute(TweetTable)


import json
import time

def loadTweets(tweetLines):

    batchRows = 500
    batchedInsertsUser = []
    batchedInsertsGeo = []
    batchedInsertsTweet = []

    while len(tweetLines) > 0:
        line = tweetLines.pop(0) 
    
        jsonobject = json.loads(line)

        newRowUser = [] 
        newRowGeo = [] 
        newRowTweet = [] 
        
        UserValues = [jsonobject['user']['id'],jsonobject['user']['name'],jsonobject['user']['screen_name'],
                    jsonobject['user']['description'],jsonobject['user']['friends_count']]       
        
        for value in UserValues:
            if value in ['',[],'null']:
                newRowUser.append(None)
            else:
                newRowUser.append(value)
                
        batchedInsertsUser.append(newRowUser)
        

        if jsonobject['geo'] != None:
            GeoValues = [str(jsonobject['geo']['coordinates'][0])+'&'+str(jsonobject['geo']['coordinates'][1]),jsonobject['geo']['type'],jsonobject['geo']['coordinates'][0],jsonobject['geo']['coordinates'][1]] 
           
            for value in GeoValues:
                if value in ['',[],'null']:
                    newRowGeo.append(None)
                else:
                    newRowGeo.append(value)
            
            batchedInsertsGeo.append(newRowGeo)        
        
        
        TweetKeys = ['id_str','created_at','text','source','in_reply_to_user_id', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'contributors']

        for key in TweetKeys:

            if jsonobject[key] in ['',[],'null']:
                newRowTweet.append(None)
            else:
                newRowTweet.append(jsonobject[key])

        if 'retweeted_status' in jsonobject.keys():
            newRowTweet.append(jsonobject['retweeted_status']['retweet_count'])
        else:
            if jsonobject['retweet_count'] in ['',[],'null']:
                newRowTweet.append(None)
            else:
                newRowTweet.append(jsonobject['retweet_count'])
        

        if jsonobject['user']['id'] in ['',[],'null']:
            newRowTweet.append(None)
        else:
            newRowTweet.append(jsonobject['user']['id'])
        

        if jsonobject['geo'] != None:
            newRowTweet.append(str(jsonobject['geo']['coordinates'][0])+'&'+str(jsonobject['geo']['coordinates'][1]))
        else:
            newRowTweet.append(None)
            
        batchedInsertsTweet.append(newRowTweet)
        

        if len(batchedInsertsUser) >= batchRows or len(tweetLines) == 0:
            c.executemany('INSERT OR REPLACE INTO User VALUES(?,?,?,?,?)', batchedInsertsUser)
            batchedInsertsUser = []
        if len(batchedInsertsGeo) >= batchRows or len(tweetLines) == 0:
            c.executemany('INSERT OR REPLACE INTO Geo VALUES(?,?,?,?)', batchedInsertsGeo)
            batchedInsertsGeo = []
        if len(batchedInsertsTweet) >= batchRows or len(tweetLines) == 0:
            c.executemany('INSERT OR REPLACE INTO Tweet VALUES(?,?,?,?,?,?,?,?,?,?,?)', batchedInsertsTweet)
            batchedInsertsTweet = []



start = time.time()

fd = open('Tweets.txt', 'r', encoding='utf8')

allLines = fd.readlines()

fd.close()

loadTweets(allLines)

end   = time.time()

print ("loadTweets took ", (end-start), ' seconds.')
print ("Table Tweet Loaded ", c.execute('SELECT COUNT(*) FROM Tweet').fetchall()[0], " rows")
print ("Table User Loaded ", c.execute('SELECT COUNT(*) FROM User').fetchall()[0], " rows")
print ("Table Geo Loaded ", c.execute('SELECT COUNT(*) FROM Geo').fetchall()[0], " rows")


#loadTweets took  108.11079049110413  seconds.
#Table Tweet Loaded  (499776,)  rows
#Table User Loaded  (447304,)  rows
#Table Geo Loaded  (11849,)  rows

#Connection not closed for the tasks in Problem 2,3 and 4

#Problem2
#Problem2Ai
#Find tweets where tweet id_str contains “44” or “77” anywhere in the column    
#query2i took  0.6250858306884766  seconds.
start = time.time()
query2i = c.execute("SELECT * FROM Tweet where id_str LIKE '%44%' OR id_str LIKE '%77%';").fetchall()
end   = time.time()


print ("query2i took ", (end-start), ' seconds.')
for row in query2i:
    print(row)
#query2i took  0.6250858306884766  seconds.
print(len(query2i))


#Problem2Aii
#Find how many unique values are there in the “in_reply_to_user_id” column
#query2ii took  0.5000169277191162  seconds.
start = time.time()
query2ii = c.execute("SELECT COUNT(DISTINCT in_reply_to_user_id) FROM Tweet;").fetchall()
end   = time.time()


print ("query2ii took ", (end-start), ' seconds.')
for row in query2ii:
    print(row)
#(90524,)
#query2ii took  0.5000169277191162  seconds.


#Problem2iii
#Find the tweet(s) with the longest text message    
#query2iii took  0.6875491142272949  seconds.
start = time.time()
query2iii = c.execute("SELECT * FROM Tweet WHERE LENGTH(text) in (SELECT MAX(LENGTH(text)) FROM Tweet);").fetchall()
end   = time.time()


print ("query2iii took ", (end-start), ' seconds.')
for row in query2iii:
    print(row)
#query2iii took  0.6875491142272949  seconds.


#Problem2iv
#Find the average longitude and latitude value for each user name
#query2iv took  2.375159502029419  seconds.
start = time.time()
query2iv = c.execute("SELECT User.name, AVG(Latitude), AVG(Longitude) FROM Tweet, User, Geo WHERE Geo.geo_id=Tweet.geo_id AND Tweet.user_id=User.user_id GROUP BY User.name ;").fetchall()
end   = time.time()


print ("query2iv took ", (end-start), ' seconds.')
for row in query2iv:
    print(row)
#query2iv took  2.375159502029419  seconds.

print(len(query2iv))


#Problem2v
#Re-execute the query in part iv) 10 times and 100 times and measure the total runtime 
#The runtime scales approximately linear when the query is performed 10 and 100 times. 
#It takes 2.375 seconds for querying once, 23.846 for 10 times, 
#and 232.7 for 100 times.

start = time.time()
for i in range(10):
    c.execute("SELECT User.name, AVG(Latitude), AVG(Longitude) FROM Tweet, User, Geo WHERE Geo.geo_id=Tweet.geo_id AND Tweet.user_id=User.user_id GROUP BY User.name ;").fetchall()
end   = time.time()

print ("query2v for 10X took ", (end-start), ' seconds.')
#query2v for 10X took  23.846107244491577  seconds.


start = time.time()
for i in range(100):
    c.execute("SELECT User.name, AVG(Latitude), AVG(Longitude) FROM Tweet, User, Geo WHERE Geo.geo_id=Tweet.geo_id AND Tweet.user_id=User.user_id GROUP BY User.name ;").fetchall()
end   = time.time()

print ("query2v for 100X took ", (end-start), ' seconds.')
#query2v for 100X took  232.70020294189453  seconds.


#Problem2B
#Write python code that is going to read the locally saved tweet data file from 1-b and perform the equivalent computation for #parts 2-i and 2-ii only
#The runtimes are 183 and 24 seconds for python versions of 2i and 2ii computations 
#which is much slower than sql queries of 0.6 and 0.5 seconds

#2i Python version
import json
import time
fd = open('Tweets.txt', 'r', encoding='utf8')
allLines = fd.readlines()
fd.close()

start = time.time()

unique = []
result = []
for line in allLines:
    jsonobject = json.loads(line)
    if '44' in str(jsonobject['id_str']) or '77' in str(jsonobject['id_str']):
        if jsonobject['id_str'] not in unique:
            unique.append(jsonobject['id_str'])  
            newRowTweet = []
            TweetKeys = ['id_str','created_at','text','source','in_reply_to_user_id', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'contributors']
            for key in TweetKeys:
                if jsonobject[key] in ['',[],'null']:
                    newRowTweet.append(None)
                else:
                    newRowTweet.append(jsonobject[key])
            if 'retweeted_status' in jsonobject.keys():
                newRowTweet.append(jsonobject['retweeted_status']['retweet_count'])
            else:
                if jsonobject['retweet_count'] in ['',[],'null']:
                    newRowTweet.append(None)
                else:
                    newRowTweet.append(jsonobject['retweet_count'])
            result.append(newRowTweet)
    
end   = time.time()
print ("search took ", (end-start), ' seconds.')

#search took  183.1206660270691  seconds.
for row in result:
    print(row)
print(len(result))

    
#2ii Python version
import json
import time
fd = open('Tweets.txt', 'r', encoding='utf8')
allLines = fd.readlines()
fd.close()

start = time.time()

dCount = {}
for line in allLines:
    jsonobject = json.loads(line)
    value = jsonobject['in_reply_to_user_id']
    if value != None:
        if value not in dCount.keys():
            dCount[value] = 0
        dCount[value] = dCount[value]+1
        
print(len(dCount))

end   = time.time()
print ("search took ", (end-start), ' seconds.')

#90524
#search took  23.949678659439087  seconds.


#Problem2C: Extra Credit
#python equivalent for 2-iii
#Runtime is 22 seconds for python version of 2iii computation
#which is also slower than sql query equivalent that took 0.7 seconds

import json
import time
fd = open('Tweets.txt', 'r', encoding='utf8')
allLines = fd.readlines()
fd.close()

start = time.time()
maxvalue = 0
tweet = []
for line in allLines:
    jsonobject = json.loads(line)
    value = len(jsonobject['text'])
    if value > maxvalue:
        maxvalue = value
        tweet = jsonobject
        
print(tweet)    
end   = time.time()
print ("search took ", (end-start), ' seconds.')

#search took  22.415924310684204  seconds.



#Problem2D: Extra Credit
#python equivalent for 2-iv    
#Runtime is 23 seconds for python version of 2iv computation
#which is also slower than sql query equivalent that took 2.4 seconds

import json
import time
fd = open('Tweets.txt', 'r', encoding='utf8')
allLines = fd.readlines()
fd.close()

start = time.time()

dCount = {}
for line in allLines:
    jsonobject = json.loads(line)
    if jsonobject['user']['name'] not in  ['',[],'null'] and jsonobject['geo'] != None :
        value = jsonobject['user']['name']
        latitude = jsonobject['geo']['coordinates'][0]
        longitude = jsonobject['geo']['coordinates'][1]
        if value not in dCount.keys():
            count = 0
            dCount[value] = [0,0,0]
        dCount[value] = [dCount[value][0] + latitude,dCount[value][1]+longitude,dCount[value][2]+1]

for key in dCount.keys():
    dCount[key][0] = dCount[key][0] / dCount[key][2]
    dCount[key][1] = dCount[key][1] / dCount[key][2]
for key in dCount.keys():
    dCount[key] = [dCount[key][0],dCount[key][1]]  

print(dCount.items())

end   = time.time()
print ("search took ", (end-start), ' seconds.') 

#search took  23.063082933425903  seconds.
        


#Problem3A
#Export the contents of the User table from a SQLite table into a sequence of INSERT statements within a file
def insertsfromtable(table):
    results = c.execute('SELECT * FROM ' + table + ';').fetchall()
    output = open('3A.txt', 'w')
    count = 0    
    for rows in results:
        insert = 'INSERT INTO ' + table + ' VALUES (' 
        for attr in rows:
            if attr == None: 
                insert = insert + 'NULL' + ', '
            else:
                if isinstance(attr, (int, float)):
                    value = str(attr)
                else: 
                    value = "'" + str(attr.encode('utf8')).replace("'", "''") + "'"
                    
                insert = insert + value + ', '
                
        unique = ''
        pre = str(count)
        for i in range(len(pre)):
            unique += chr(ord('a')+ int( pre[i] ) )
        insert = insert +  "'" + unique + "', "
        count += 1

        insert = insert[:-2] + '); \n'
        output.write(insert)
    output.close()
    
start = time.time()
insertsfromtable('User')
  
end   = time.time()
print ("export took ", (end-start), ' seconds.')

#export took  9.234607458114624  seconds.


#Problem3B
#Create INSERT for the User table by reading/parsing data from the local tweet file saved earlier
#Exporting the contents of the User table from a SQLite table is faster 
#which took 9 seconds compared to reading/parsing data from the local tweet 
#file which took 64 seconds.

def insertsfromfile(table):
    fd = open('Tweets.txt', 'r', encoding='utf8')
    allLines = fd.readlines()
    fd.close()
    output = open('3B.txt', 'w')
    count = 0
    for line in allLines:
        jsonobject = json.loads(line)
        insert = 'INSERT INTO ' + table + ' VALUES (' 
        UserValues = [jsonobject['user']['id'],jsonobject['user']['name'],jsonobject['user']['screen_name'],
                    jsonobject['user']['description'],jsonobject['user']['friends_count']] 
        for attr in UserValues :
            if attr == None: 
                insert = insert + 'NULL' + ', '
            else:
                if isinstance(attr, (int, float)):
                    value = str(attr)
                else: 
                    value = "'" + str(attr.encode('utf8')).replace("'", "''") + "'"
                    
                insert = insert + value + ', '

        unique = ''
        pre = str(count)
        for i in range(len(pre)):
            unique += chr(ord('a')+ int( pre[i] ) )
        insert = insert +  "'" + unique + "', "
        count += 1
        
        insert = insert[:-2] + '); \n'
        output.write(insert)
    output.close()
    
start = time.time()
insertsfromfile('User')

end   = time.time()
print ("export took ", (end-start), ' seconds.')  

#export took  64.23220133781433  seconds.


#Problem4A
#Export all three tables from the database into a |-separated text file
contents_geo = c.execute("SELECT * FROM Geo;").fetchall()
output = open('TableGeo.txt', 'w')
for rows in contents_geo:
    insert = ''
    for attr in rows:
        if attr == None:
            insert = insert + 'NULL' + '| '
        else:
            if isinstance(attr, int):
                value = str(attr)
            elif isinstance(attr, float):
                value = str(round(attr,4))
            else:
                value = "'" + str(attr.encode('utf8')).replace("'", "''") + "'"
            insert = insert + value + '| '
        
    insert = insert[:-2] + '\n'
    output.write(insert)
output.write('Unknown'+ '| '+ 'NULL' + '| '+ 'NULL' + '| '+ 'NULL')
output.close()


#Problem4B
#For the Geo table, create a single default entry for the ‘Unknown’ location and 
#round longitude and latitude to a maximum of 4 digits after the decimal
#There are 487793 unknown and 11983 known locations.
#Hence, about 2.4% of total tweets has locations available

contents_tweet = c.execute("SELECT * FROM Tweet;").fetchall()
output = open('TableTweet.txt', 'w')
unknown_count = 0
known_count = 0
for rows in contents_tweet:
    insert = ''
    for attr in rows[:10]:
        if attr == None:
            insert = insert + 'NULL' + '| '
        else:
            if isinstance(attr, (int, float)):
                value = str(attr)
            else:
                value = "'" + str(attr.encode('utf8')).replace("'", "''") + "'"
            insert = insert + value + '| '
    if rows[10] == None:
        insert = insert + 'Unknown' + '| '
        unknown_count += 1
    else:
        value = "'" + str(rows[10].encode('utf8')).replace("'", "''") + "'"
        insert = insert + value + '| '
        known_count += 1
        
    insert = insert[:-2] + '\n'
    output.write(insert)
output.close()

print(unknown_count)
print(known_count)
print('There are '+ str(unknown_count) +' unknown locations')
print('There are '+ str(known_count) +' unknown locations')

#487793
#11983
#There are 487793 unknown locations
#There are 11983 unknown locations

#Problem4C
#For the Tweet table, replace NULLs by a reference to ‘Unknown’ entry
contents_user = c.execute("SELECT * FROM User;").fetchall()
output = open('TableUser.txt', 'w')
true = 0
false = 0
for rows in contents_user:
    insert = ''
    for attr in rows:
        if attr == None:
            insert = insert + 'NULL' + '| '
        else:
            if isinstance(attr, (int, float)):
                value = str(attr)
            else:
                value = "'" + str(attr.encode('utf8')).replace("'", "''") + "'"
            insert = insert + value + '| '
    name = str(rows[1])  
    screen_name = str(rows[2])
    description = str(rows[3]) 
    if name in screen_name or name in description:
        insert = insert + 'True' + '| '
    if name not in screen_name and name not in description:
        insert = insert + 'False' + '| '

    insert = insert[:-2] + '\n'
    output.write(insert)
output.close()


c.close()
conn.commit()
conn.close()




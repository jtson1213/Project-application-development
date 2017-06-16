
# CSC455 FINAL
# Jun Tae Son

## working directory
import os
os.getcwd()

## change directory
os.chdir("C:\\Users\\rkfql\\Downloads")

#####################################
# Q1
#####################################

# a.	Create a 3rd table incorporating the Geo table (in addition to tweet 
# and user tables that you already have from HW4 and HW5) and extend your schema accordingly. 
# Once again, you do not need to use ALTER TABLE, just re-make your schema.
# You will need to generate an ID for the Geo table primary key 
# (you may use any value or reasonable combination of values as long as it is unique) 
# for that table and link it to the Tweet table (foreign key should be in the Tweet). 
# In addition to the primary key column, the geo table should have “type”, “longitude” and “latitude” columns.

geoTable = '''CREATE TABLE geo
(
    id NUMBER(25),  
    type VARCHAR(10) NOT NULL,
    longitude NUMBER(15,6) NOT NULL,
    latitude NUMBER(15,6) NOT NULL,
    
    CONSTRAINT geo_pk
        PRIMARY KEY (id)       
);
'''

userTable = ''' CREATE TABLE user
(
    id INTEGER(25),
    name VARCHAR(20),
    screen_name VARCHAR(40),
    description VARCHAR(200),
    friends_count INTEGER(5),

    CONSTRAINT user_pk
        PRIMARY KEY (id)
);
'''

tweetTable = '''CREATE TABLE tweet
(
    user_id NUMBER(25),
    geo_id NUMBER(25),
    created_at DATE,
    id_str VARCHAR(25),
    text VARCHAR(140),
    source VARCHAR(200),
    in_reply_to_user_id VARCHAR(20),
    in_reply_to_screen_name VARCHAR(60),
    in_reply_to_status_id NUMBER(20),
    retweet_count NUMBER(10),
    contributors VARCHAR(200),
    
    CONSTRAINT tweet_pk
        PRIMARY KEY (id_str),

    CONSTRAINT tweet_fk1
        FOREIGN KEY (user_id)
        REFERENCES user(id),
        
    CONSTRAINT tweet_fk2
        FOREIGN KEY (geo_id)
        REFERENCES geo(id)
);
'''

import sqlite3
# Create Geo Table
conn = sqlite3.connect("final.db")
cursor = conn.cursor()
cursor.execute('DROP TABLE IF EXISTS geo;')
cursor.execute(geoTable)

cursor.execute('DROP TABLE IF EXISTS user;')
cursor.execute(userTable)

cursor.execute('DROP TABLE IF EXISTS tweet;')
cursor.execute(tweetTable)



# b.	Use python to download from the web and save to a local text file 
# (not into database yet, just to text file) at least 1,000,000 lines worth of tweets. 
# Test your code with fewer rows first and only time it when you know it works. 
# Report how long did it take.

# NOTE: Do not call read() or readlines() without any parameters at any point. 
# That command will attempt to read the entire file which is too much data.

import urllib2
import time
webFD=urllib2.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")
n = 1000000 # the number of records that you want to load
f = open("Q1b.txt","a")
success_count = 0

start_1b = time.time()
for i in xrange(n):
    try:
        lines = webFD.readline()
        tweets = lines.decode('utf8')
        f.write(tweets.encode('utf8'))
        success_count +=1
    except ValueError:
        pass
end_1b = time.time()


time_diff_1b = end_1b - start_1b
print("{} lines of tweet data were successfully saved!".format(success_count))
print("This process took {} seconds to operate.".format(round(time_diff_1b,2)))
webFD.close() 

'''
This process took 548.67 seconds to operate.

'''

# c.	Repeat what you did in part-b, but instead of saving tweets to the file, 
# populate the 3-table schema that you created in SQLite. Be sure to execute commit 
# and verify that the data has been successfully loaded (report loaded row counts for each of the 3 tables).
# How long did this step take?

import urllib2
import json
import time

webFD=urllib2.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")
n = 1000000 # the number of records that you want to load

start_1c = time.time()
for i in xrange(n):
    try:
        lines = webFD.readline()
        tweets = lines.decode('utf8')
        tweet = json.loads(tweets)
        # geo table
        if tweet['geo'] != None:
            cursor.execute("INSERT OR IGNORE INTO geo VALUES (?,?,?,?);", 
                           (tweet['place']['id'], tweet['geo']['type'], tweet['geo']['coordinates'][0], 
                            tweet['geo']['coordinates'][1]))
            conn.commit()
        else:
            pass
        # tweet table
        if tweet['place'] != None:
            cursor.execute("INSERT OR IGNORE INTO tweet VALUES (?,?,?,?,?,?,?,?,?,?,?);", 
                           [tweet['user']['id'], tweet['place']['id'],tweet['created_at'], tweet['id_str'], tweet['text'],
                            tweet['source'], tweet['in_reply_to_user_id'], tweet['in_reply_to_screen_name'],
                            tweet['in_reply_to_status_id'], tweet['retweet_count'], tweet['contributors']])
            conn.commit()
        else: 
            cursor.execute("INSERT OR IGNORE INTO tweet VALUES (?,?,?,?,?,?,?,?,?,?,?);", 
                           [tweet['user']['id'], None, tweet['created_at'], tweet['id_str'], tweet['text'],
                            tweet['source'],tweet['in_reply_to_user_id'],tweet['in_reply_to_screen_name'],
                            tweet['in_reply_to_status_id'], tweet['retweet_count'], tweet['contributors']])
            conn.commit()
       # user table
        cursor.execute("INSERT OR IGNORE INTO user VALUES (?,?,?,?,?);", 
                       (tweet['user']['id'], tweet['user']['name'], tweet['user']['screen_name'], 
                        tweet['user']['description'], tweet['user']['friends_count']))
        conn.commit()
 
    except (ValueError, TypeError):
        pass
        
end_1c = time.time()

time_diff_1c = end_1c - start_1c
print("{} number of rows were successfully loaded in geo table!".format(conn.execute("SELECT COUNT(*) FROM geo").fetchall()[0][0]))
print("{} number of rows were successfully loaded in user table!".format(conn.execute("SELECT COUNT(*) FROM user").fetchall()[0][0]))
print("{} number of rows were successfully loaded in tweet table!".format(conn.execute("SELECT COUNT(*) FROM tweet").fetchall()[0][0]))
print("This process took {} seconds to operate.".format(round(time_diff_1c,2)))
webFD.close() 

'''
This process took 31182.27 seconds to operate.

'''


# d. Use your locally saved tweet file (created in part-b) to repeat the database population step from part-c. 
# That is, load 1,000,000 tweets into the 3-table database using your saved file with tweets 
# (do not use the URL to read twitter data). How does the runtime compare with part-c?

import json
import time

file=open("Q1b.txt", "r")
lines = file.readlines()

start_1d = time.time()
for line in lines:
    try:
        tweet = json.loads(line)
        # geo table
        if tweet['geo'] != None:
            cursor.execute("INSERT OR IGNORE INTO geo VALUES (?,?,?,?);", 
                           (tweet['place']['id'], tweet['geo']['type'], tweet['geo']['coordinates'][0], 
                            tweet['geo']['coordinates'][1]))
            conn.commit()
        else:
            pass
        # tweet table
        if tweet['place'] != None:
            cursor.execute("INSERT OR IGNORE INTO tweet VALUES (?,?,?,?,?,?,?,?,?,?,?);", 
                           [tweet['user']['id'], tweet['place']['id'],tweet['created_at'], tweet['id_str'], tweet['text'],
                            tweet['source'], tweet['in_reply_to_user_id'], tweet['in_reply_to_screen_name'],
                            tweet['in_reply_to_status_id'], tweet['retweet_count'], tweet['contributors']])
            conn.commit()
        else: 
            cursor.execute("INSERT OR IGNORE INTO tweet VALUES (?,?,?,?,?,?,?,?,?,?,?);", 
                           [tweet['user']['id'], None, tweet['created_at'], tweet['id_str'], tweet['text'],
                            tweet['source'],tweet['in_reply_to_user_id'],tweet['in_reply_to_screen_name'],
                            tweet['in_reply_to_status_id'], tweet['retweet_count'], tweet['contributors']])
            conn.commit()
       # user table
        cursor.execute("INSERT OR IGNORE INTO user VALUES (?,?,?,?,?);", 
                       (tweet['user']['id'], tweet['user']['name'], tweet['user']['screen_name'], 
                        tweet['user']['description'], tweet['user']['friends_count']))
        conn.commit()
 
    except (ValueError, TypeError):
        pass
        
end_1d = time.time()

time_diff_1d = end_1d - start_1d
print("{} number of rows were successfully loaded in geo table!".format(conn.execute("SELECT COUNT(*) FROM geo").fetchall()[0][0]))
print("{} number of rows were successfully loaded in user table!".format(conn.execute("SELECT COUNT(*) FROM user").fetchall()[0][0]))
print("{} number of rows were successfully loaded in tweet table!".format(conn.execute("SELECT COUNT(*) FROM tweet").fetchall()[0][0]))
print("This process took {} seconds to operate.".format(round(time_diff_1d,2)))
 
'''
This process took 27074.17 seconds to operate.
The runtime became a little shorter, but it still took a lot of time to operate.

'''


# e.	Re-run the previous step with batching size of 1000 
# (i.e. by inserting 1000 rows at a time with executemany). 
# How does the runtime compare when batching is used?

import json
import time


                                                                    
start_1e =time.time()     
file=open("Q1b.txt", "r")
tweetLines = file.readlines()

batchRows=1000 
batchedInserts_geo=[]
batchedInserts_user=[]
batchedInserts_tweet=[] 

while len(tweetLines)>0:
    line=tweetLines.pop(0)
    try:
        tweetDict=json.loads(line)
    except ValueError:
        pass
    
    try:
        # geo table
        newRow_geo = []
        
        if tweetDict['geo'] is None:
            newRow_geo.append(None)
            newRow_geo.append(None)
            newRow_geo.append(None)
            newRow_geo.append(None)
        else: 
            newRow_geo.append(tweetDict['place']['id'])
            newRow_geo.append(tweetDict['geo']['type'])
            newRow_geo.append(tweetDict['geo']['coordinates'][0])
            newRow_geo.append(tweetDict['geo']['coordinates'][1])
        batchedInserts_geo.append(newRow_geo)
    
        # user table
        newRow_user = [] 
        tweetKeys_user=['id','name','screen_name','description','friends_count']
        
        for key in tweetKeys_user:
            if tweetDict['user'][key] in ['',[],'null']:
                newRow_user.append(None)
            else:
                newRow_user.append(tweetDict['user'][key])
                
        batchedInserts_user.append(newRow_user)
        
        # tweet table
        newRow_tweet = []
        tweetKeys_tweet=['created_at','id_str','text','source','in_reply_to_user_id',
                         'in_reply_to_screen_name', 'in_reply_to_status_id', 'retweet_count', 'contributors']
        
        if tweetDict['user'] is None:
            newRow_tweet.append(None)
        else: 
            newRow_tweet.append(tweetDict['user']['id'])
            
        if tweetDict['place'] is None:
            newRow_tweet.append(None)
        else: 
            newRow_tweet.append(tweetDict['place']['id'])
                                        
        for key in tweetKeys_tweet:
            if tweetDict[key] in ['',[],'null']:
                newRow_tweet.append(None)
            else:
                newRow_tweet.append(tweetDict[key])
                
        batchedInserts_tweet.append(newRow_tweet)                                             
                                                   
        # insert into tables
        if len(batchedInserts_geo) >= batchRows or len(tweetLines) == 0:
            conn.executemany("INSERT OR IGNORE INTO geo VALUES (?,?,?,?);", batchedInserts_geo)
            conn.commit()
            batchedInserts_geo = []
                                                            
        if len(batchedInserts_user) >= batchRows or len(tweetLines) == 0:
            conn.executemany("INSERT OR IGNORE INTO user VALUES (?,?,?,?,?);", batchedInserts_user)
            conn.commit()
            batchedInserts_user = []
                                                                
        if len(batchedInserts_tweet) >= batchRows or len(tweetLines) == 0:
            conn.executemany("INSERT OR IGNORE INTO tweet VALUES (?,?,?,?,?,?,?,?,?,?,?);", batchedInserts_tweet)
            conn.commit()
            batchedInserts_tweet = []
    except TypeError:
        pass                                                                    
file.close()
end_1e =time.time()

time_diff_1e = end_1e - start_1e
print("{} number of rows were successfully loaded in geo table!".format(conn.execute("SELECT COUNT(*) FROM geo").fetchall()[0][0]))
print("{} number of rows were successfully loaded in user table!".format(conn.execute("SELECT COUNT(*) FROM user").fetchall()[0][0]))
print("{} number of rows were successfully loaded in tweet table!".format(conn.execute("SELECT COUNT(*) FROM tweet").fetchall()[0][0]))
print("This process took {} seconds to operate.".format(round(time_diff_1e,2)))
 

'''
This process took 1074.64 seconds to operate.
The run time with batching size of 1000 is much faster than other two methods.

'''




#####################################
# Q2
#####################################

# a. Write and execute SQL queries to do the following. Don’t forget to report 
# the running times in each part and the code you used.

import sqlite3
import time
conn = sqlite3.connect("final.db")
cursor = conn.cursor()


# i.	Find tweets where tweet id_str contains “44” or “77” anywhere in the column
start = time.time()
q2_ai = conn.execute("SELECT text FROM tweet WHERE id_str LIKE '%44%' OR id_str LIKE '%77%';").fetchall()
end = time.time()
time_diff = end - start
print("This process took {} seconds to operate.".format(round(time_diff,2)))
print("{} number of tweets are selected.".format(len(q2_ai)))
print("Print the first five rows:")
for i in xrange(5):
    print(q2_ai[i][0].encode('utf8'))


'''
This process took 1.5 seconds to operate.

'''


# ii.	Find how many unique values are there in the “in_reply_to_user_id” column

start = time.time()
q2_aii = conn.execute("SELECT COUNT(DISTINCT in_reply_to_user_id) FROM tweet;").fetchall()
end = time.time()
time_diff = end - start
print("This process took {} seconds to operate.".format(round(time_diff,2)))
print("There are {} number of unique values in the 'in_reply_to_user_id' column".format(q2_aii[0][0]))

'''
This process took 8.99 seconds to operate.

'''

# iii.	Find the tweet(s) with the shortest, longest and average length text message.

start = time.time()
q2_aiii_shortest = conn.execute("SELECT LENGTH(text), text FROM tweet WHERE LENGTH(text) = (SELECT MIN(LENGTH(text)) FROM tweet);").fetchall()
q2_aiii_longest = conn.execute("SELECT LENGTH(text), text FROM tweet WHERE LENGTH(text) = (SELECT MAX(LENGTH(text)) FROM tweet);").fetchall()
q2_aiii_average = conn.execute("SELECT LENGTH(text), text FROM tweet WHERE LENGTH(text) = (SELECT ROUND(AVG(LENGTH(text)),0) FROM tweet);").fetchall()
end = time.time()
time_diff = end - start
print("This process took {} seconds to operate.".format(round(time_diff,2)))
print("The tweet(s) with the shortest length of text message is: {}".format(q2_aiii_shortest[0][1].encode('utf8')))
print("The tweet(s) with the longest length of text message is: {}".format(q2_aiii_longest[0][1].encode('utf8')))
print("The tweet(s) with the average length of text message is: {}".format(q2_aiii_average[0][1].encode('utf8')))

'''
This process took 8.99 seconds to operate.

'''


# iv.	Find the average longitude and latitude value for each user name.

start = time.time()
q2_aiv = conn.execute("SELECT user.name, AVG(geo.longitude), AVG(geo.latitude), tweet.user_id, tweet.geo_id FROM user, geo, tweet WHERE user.id = tweet.user_id AND geo.id = tweet.geo_id GROUP BY user.name;").fetchall()
end = time.time()
time_diff = end - start

print("This process took {} seconds to operate.".format(round(time_diff,2)))
print("{} number of rows were selected.".format(len(q2_aiv)))

'''
This process took 13.59 seconds to operate.

'''


# v.	Re-execute the query in part iv) 10 times and 100 times and measure the total runtime 
#       (just re-run the same exact query multiple times using a for-loop). 
#       Does the runtime scale linearly? (i.e., does it take 10X and 100X as much time?)

# 10 times
start = time.time()
for i in xrange(10):
    conn.execute("SELECT text FROM tweet WHERE id_str LIKE '%44%' OR id_str LIKE '%77%';").fetchall()
end = time.time()
time_diff = end - start
print("This process took {} seconds to operate.".format(round(time_diff,2)))

'''
This process took 16.63 seconds to operate.

'''

# 100 times  
start = time.time()    
for i in xrange(100):
    conn.execute("SELECT text FROM tweet WHERE id_str LIKE '%44%' OR id_str LIKE '%77%';").fetchall()
end = time.time()
time_diff = end - start
print("This process took {} seconds to operate.".format(round(time_diff,2)))

'''
This process took 189.97 seconds to operate.
The runtime does not scale linearly.

'''



# b. Write python code that is going to read the locally saved tweet data file 
# from 1-b and perform the equivalent computation for parts 2-i and 2-ii only. 
# How does the runtime compare to the SQL queries?

# i.	Find tweets where tweet id_str contains “44” or “77” anywhere in the column
start_b1 = time.time()
file = open('Q1b.txt', 'r')
lines = file.readlines()
file.close() 

result = []

for line in lines:
    tweet = json.loads(line)
    bi_key = ['id_str', 'text']
    lst = []
    for key in bi_key:
        if tweet[key] in ['',[],'null']:
            lst.append(None)
        else:
            lst.append(tweet[key])
    result.append(lst)
    
b1 = []
for i in xrange(len(result)):
    if '44' in result[i][0] or '77' in result[i][0]:
        b1.append(result[i])

end_b1 = time.time()
time_diff_b1 = end_b1 - start_b1
print("This process took {} seconds to operate.".format(round(time_diff_b1,2)))
print("{} number of tweets are selected.".format(len(b1)))

'''
This process took 167.43 seconds to operate.

'''


# ii.	Find how many unique values are there in the “in_reply_to_user_id” column

start_b2 = time.time()
file = open('Q1b.txt', 'r')
lines = file.readlines()
file.close() 

result = []

for line in lines:
    tweet = json.loads(line)
    if tweet['in_reply_to_user_id'] != None:
        result.append(tweet['in_reply_to_user_id'])

end_b2 = time.time()
time_diff_b2 = end_b2 - start_b2
print("This process took {} seconds to operate.".format(round(time_diff_b2,2)))
print("There are {} number of unique values in the 'in_reply_to_user_id' column".format(len(result)))

'''
This process took 169.18 seconds to operate.
the SQL queries are faster.

'''


#####################################
# Q3
#####################################

# a.Export the contents of the User table from a SQLite table into a sequence 
# of INSERT statements within a file. This is very similar to what you did in Assignment 4. 
# However, you have to add a unique ID column which has to be a string (you cannot use numbers). 
# Hint: you can replace digits with letters, e.g., chr(ord('a')+1) gives you a 'b' and chr(ord('a')+2) returns a 'c'

import sqlite3
import random
import string

conn = sqlite3.connect("final.db")
cursor = conn.cursor()

start_3a = time.time()
export_user = conn.execute("SELECT * FROM user;").fetchall()
for record in export_user:
    user_records = (''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6)), record[0], record[1], record[2], record[3], record[4])
    #print(user_records)
    with open ("generateInsert_user.txt","a") as outfile:
        outfile.write("INSERT OR IGNORE INTO user VALUES {};".format(user_records))
end_3a = time.time()   

time_diff_3a = end_3a - start_3a
print("This process took {} seconds to operate.".format(round(time_diff_3a,2)))

'''
This process took 6423.09 seconds to operate.

'''

# b. Create a similar collection of INSERT for the User table by reading/parsing data 
# from the local tweet file that you have saved earlier. 
# How do these compare in runtime? Which method was faster?

start_3b = time.time()
file = open('Q1b.txt', 'r')
lines = file.readlines()
file.close() 

for line in lines:
    tweet = json.loads(line)
    user_rec = (''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6)), tweet['user']['id'], tweet['user']['name'], tweet['user']['screen_name'], tweet['user']['description'], tweet['user']['friends_count'])
    with open ("generateInsert_user3B.txt","a") as outfile:
        outfile.write("INSERT OR IGNORE INTO user VALUES {};".format(user_rec))

end_3b = time.time()

time_diff_3b = end_3b - start_3b
print("This process took {} seconds to operate.".format(round(time_diff_3b,2)))


'''
This process took 8275.12 seconds to operate.

'''




#####################################
# Q4
#####################################
# Export all three tables (Tweet, User and Geo tables) from the database into a |-separated text file 
# (each value in a row should be separated by |). You do not generate INSERT statements, just raw |-separated text data.
import sqlite3

conn = sqlite3.connect("final.db")
cursor = conn.cursor()

export_geo = conn.execute("SELECT * FROM geo;").fetchall()
export_user = conn.execute("SELECT * FROM user;").fetchall()
export_tweet = conn.execute("SELECT * FROM tweet;").fetchall()

f_geo = open("geoTable.txt","w")
f_user = open("userTable.txt","w")
f_tweet = open("tweetTable.txt","w")

for line in export_geo:
    for word in line:
        try:
            f_geo.write(str(word))
        except UnicodeEncodeError:
            f_geo.write(word.encode('utf8'))
        f_geo.write('|')
    f_geo.write('\n')
f_geo.close()

for line in export_user:
    for word in line:
        try:
            f_user.write(str(word))
        except UnicodeEncodeError:
            f_user.write(word.encode('utf8'))
        f_user.write('|')
    f_user.write('\n')
f_user.close()

for line in export_tweet:
    for word in line:
        try:
            f_tweet.write(str(word))
        except UnicodeEncodeError:
            f_tweet.write(word.encode('utf8'))
        f_tweet.write('|')
    f_tweet.write('\n')
f_tweet.close()


# a. For the Geo table, add a new column with distance from (41.878668, -87.625555) 
# which is the location of CDM. You can simply treat it as a point-to-point Euclidean distance 
# (although bonus points for finding a real distance in miles) and round the longitude and latitude columns 
# to a maximum of 4 digits after the decimal.

import numpy as np
import math

def DistFunction(point):
    point1=np.array((41.878668,-87.625555))
    point2=np.array(point)
    try:
        d = round(math.sqrt((point1[0]-point2[0])**2+(point1[1]-point2[1])**2),4)
        return d
    except:
        return "Error"

start = time.time()
export_geo = conn.execute("SELECT * FROM geo;").fetchall()
location = conn.execute("SELECT latitude, latitude FROM geo;").fetchall()


for entry in export_geo:
    result = DistFunction((entry[2],entry[3]))
    with open("generateInsert4a.txt", "a") as outfile:
        outfile.write("{}|{}|{}|{}|{}\n".format(entry[0],entry[1],round(entry[2],4),round(entry[3],4),result))
end = time.time()
time_diff = end - start
print("This process took {} seconds to operate.".format(round(time_diff,2)))


'''
This process took 46.31 seconds to operate.
'''


# b. For the Tweet table, add two new columns from the User table 
# (“name” and “screen_name” in addition to existing tables. Report how many known/unknown 
# locations there were in total (e.g., 50,000 known, 950,000 unknown,  5% locations are available)

start = time.time()
export_tweet = conn.execute("SELECT * FROM tweet;").fetchall()
export_user = conn.execute("SELECT id, name, screen_name FROM user;").fetchall()


id_user = []
for entry_u in export_user:
    id_user.append(entry_u[0])

result = []
for entry_t in export_tweet:
    for word in entry_t:
        result.append(word)
    if entry_t[0] not in id_user:
        result.append(None)
        result.append(None)
        break
    else:
        for entry_u in export_user:
            if entry_t[0] == entry_u[0]:
                result.append(entry_u[1])
                result.append(entry_u[2])
                break     
            else: 
                pass
    try:
        with open("generateInsert4b.txt","a") as outfile:
            outfile.write("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}\n".format(result[0],result[1],result[2],result[3],result[4].encode('utf8'),result[5],result[6],result[7],result[8],result[9],result[10],result[11],result[12]))
        result=[]
    except UnicodeEncodeError:
        if result[11] is None:
            with open("generateInsert4b.txt","a") as outfile:
                outfile.write("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}\n".format(result[0],result[1],result[2],result[3],result[4].encode('utf8'),result[5].encode('utf8'),result[6],result[7],result[8],result[9],result[10],result[11],result[12]))
            result=[]   
        else:
            with open("generateInsert4b.txt","a") as outfile:
                outfile.write("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}\n".format(result[0],result[1],result[2],result[3],result[4].encode('utf8'),result[5].encode('utf8'),result[6],result[7],result[8],result[9],result[10],result[11].encode('utf8'),result[12]))
            result=[]
      
        
response = conn.execute("SELECT COUNT(geo_id), COUNT(*) FROM tweet;").fetchall()
a = response[0][0]
b = response[0][1]-response[0][0]
c = float(a)/(float(a)+float(b))
print("Known location: {}".format(a))
print("Unknown location: {}".format(b))
print("{}% locations are available.".format(round(c,2)))

end = time.time()
time_diff = end - start
print("This process took {} seconds to operate.".format(round(time_diff,2)))



'''
Known location: 26321
Unknown location: 972845
0.03% locations are available.

I stopped the process because it took more than 18297.67 seconds to operate.
'''


# c. For the User table file add a column that specifies how many tweets by that user are currently in the database. 
# That is, your output file should contain all of the columns from the User table, plus the new column with tweet count.
# You do not need to modify the original User table, just the output text file. What is the name of the user with most tweets?

start = time.time()
export_user = conn.execute("SELECT * FROM user;").fetchall()
records = conn.execute("SELECT user.name, tweet.user_id, COUNT(*) FROM user, tweet WHERE user.id = tweet.user_id GROUP BY tweet.user_id ORDER BY COUNT(*) DESC;").fetchall()

id_user = []
for r in records:
    id_user.append(r[1])


result = []
for entry_u in export_user:
    for word in entry_u:
        result.append(word)
    if entry_u[0] not in id_user:
        result.append(None)
        break
    else:
        for r in records:
            if entry_u[0] == r[1]:
                result.append(r[2])
                break     
            else: 
                pass
               
    try:
        with open("generateInsert4c.txt", "a") as outfile:
            outfile.write("{}|{}|{}|{}|{}|{}\n".format(result[0],result[1],result[2],result[3],result[4],result[5]))
        result=[]
    except UnicodeEncodeError:
        if result[3] is None and result[1] is not None:
            with open("generateInsert4c.txt", "a") as outfile:
                outfile.write("{}|{}|{}|{}|{}|{}\n".format(result[0],result[1].encode('utf8'),result[2],result[3],result[4],result[5]))
            result=[]
        elif result[3] is not None and result[1] is None:
            with open("generateInsert4c.txt", "a") as outfile:
                outfile.write("{}|{}|{}|{}|{}|{}\n".format(result[0],result[1],result[2],result[3].encode('utf8'),result[4],result[5]))
            result=[]
        else:
            with open("generateInsert4c.txt", "a") as outfile:
                outfile.write("{}|{}|{}|{}|{}|{}\n".format(result[0],result[1].encode('utf8'),result[2],result[3].encode('utf8'),result[4],result[5]))
            result=[]
        
end = time.time()
time_diff = end - start
print("This process took {} seconds to operate.".format(round(time_diff,2)))
print("The name of the user with the most tweets is : {}".format(records[0][0]))  

'''

The name of the user with the most tweets is : Alma Arafat

I stopped the process because it took more than 4243.67 seconds to operate.

'''


conn.close()








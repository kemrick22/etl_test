#Import Dependencies
import requests
import json
from pprint import pprint
import mysql.connector

mydb = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  passwd="root"
)


mycursor = mydb.cursor()

mycursor.execute('drop database if exists nfl_project')
mycursor.execute("CREATE DATABASE nfl_project")


mydb = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  passwd="root",
  database="nfl_project"
)


#=========================================
# DROP TABLES
#=========================================

print('1.  NFL_PROJECT Schema: Dropping Tables')
drop_cursor = mydb.cursor()


#order is important when dropping due to fk constraints
drop_cursor.execute('drop table if exists nfl_project.venues')
drop_cursor.execute('drop table if exists nfl_project.games')
drop_cursor.execute('drop table if exists nfl_project.pricing')



#=========================================
# CREATE TABLES
#=========================================

print('------------------------')
print('2.  NFL_PROJECT Schema: Creating tables')

mycursor = mydb.cursor()

#states table
mycursor.execute \
("create table nfl_project.venues \
  (venue_id int(10) not null\
  ,lat float \
  ,lon float \
  ,primary key (venue_id))")

mycursor.execute \
("create table nfl_project.games \
  (game_id int(10) not null\
  ,title varchar(255)\
  ,team1 varchar(255) \
  ,team2 varchar(255) \
  ,venue_id int(10)\
  ,utcdate varchar(255)\
  ,primary key (game_id))")

mycursor.execute \
("create table nfl_project.pricing \
  (game_id int(10) not null\
  ,low_price float \
  ,med_price float \
  ,high_price float \
  ,primary key (game_id))")

#Define API URL and Key
base = "https://api.seatgeek.com/2/events?"
key="MTY2Njc1Njd8MTU1ODA1NzIyNy44OQ"
start_date="2019-09-05"
sport="nfl"

#Build API String
string=f"{base}&client_id={key}&datetime_utc.gte={start_date}&type={sport}&per_page=500"
data=requests.get(string).json()

counter=0
for game in data["events"]:
    counter+=1
    #Venue Data
    venue_id=game["venue"]["id"]
    lat=game["venue"]["location"]["lat"]
    lon=game["venue"]["location"]["lon"]
    venue_data=(venue_id,lat,lon)

    #Game Data
    game_id=game["id"]
    title = game["title"]
    team1=game["performers"][0]["name"]
    team2=game["performers"][1]["name"]
    date_time=game["datetime_utc"]
    popularity=game['score']

    game_data=(game_id,title,team1,team2,date_time,venue_id)

    #Pricing Data
    low_price=game["stats"]["lowest_price"]
    med_price=game["stats"]["median_price"]
    high_price=game["stats"]["highest_price"]

    pricing_data=(game_id,low_price,med_price,high_price)

    # venueInsert = f"IF NOT EXISTS (SELECT * FROM venues WHERE venue_id ={venue_id} )\
    #                         INSERT INTO venues (venue_id, lat, lon)\
    #                         VALUES (%s, %s, %s)"
    venueInsert = "INSERT IGNORE INTO venues (venue_id, lat, lon) VALUES (%s, %s, %s)"                
    gameInsert = "INSERT INTO games (game_id, title,team1, team2, utcdate, venue_id) VALUES ( %s, %s, %s, %s, %s,%s)"
    priceInsert = "INSERT INTO pricing (game_id, low_price, med_price, high_price) VALUES (%s, %s, %s, %s)"



    mycursor.execute(venueInsert, venue_data)
    mycursor.execute(gameInsert, game_data)
    mycursor.execute(priceInsert, pricing_data)
    mydb.commit()
    print(f"Imported Game {counter}")

print('------------------------')
print('--- Job Completed ---')



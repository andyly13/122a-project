import csv
import sys
import os
import mysql.connector
from mysql.connector.errors import Error

def connect(UserName, PassWord, DataBase_Name):
    try:
        connection = mysql.connector.connect(user=UserName, password=PassWord, database=DataBase_Name)
        cursor = connection.cursor()
        return cursor, connection 
    except mysql.connector.Error as e:
        print(e)
        return None, None

def DB_creation(cursor, DataBase):

    #all tables here
    cursor.execute('DROP DATABASE IF EXISTS `%s`' %DataBase)
    cursor.execute('CREATE DATABASE `%s`' %DataBase)
    cursor.execute('USE `%s`' %DataBase)

    user = """CREATE TABLE Users (
    uid INT,
    email TEXT NOT NULL,
    joined_date DATE NOT NULL,
    nickname TEXT NOT NULL,
    street TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    genres TEXT,
    PRIMARY KEY (uid))"""

    producers = """CREATE TABLE Producers (
    uid INT,
    bio TEXT,
    company TEXT,
    PRIMARY KEY (uid),
    FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE)"""

    viewers = """CREATE TABLE Viewers (
    uid INT,
    subscription ENUM('free', 'monthly', 'yearly'),
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    PRIMARY KEY (uid),
    FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE)"""

    releases = """CREATE TABLE Releases (
    rid INT,
    producer_uid INT NOT NULL,
    title TEXT NOT NULL,
    genre TEXT NOT NULL,
    release_date DATE NOT NULL,
    PRIMARY KEY (rid),
    FOREIGN KEY (producer_uid) REFERENCES Producers(uid) ON DELETE CASCADE)"""

    movies = """CREATE TABLE Movies (
    rid INT,
    website_url TEXT,
    PRIMARY KEY (rid),
    FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE)"""

    series = """CREATE TABLE Series (
    rid INT,
    introduction TEXT,
    PRIMARY KEY (rid),
    FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE)"""

    videos = """CREATE TABLE Videos (
    rid INT,
    ep_num INT NOT NULL,
    title TEXT NOT NULL,
    length INT NOT NULL,
    PRIMARY KEY (rid, ep_num),
    FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE)"""

    sessions = """CREATE TABLE Sessions (
    sid INT,
    uid INT NOT NULL,
    rid INT NOT NULL,
    ep_num INT NOT NULL,
    initiate_at DATETIME NOT NULL,
    leave_at DATETIME NOT NULL,
    quality ENUM('480p', '720p', '1080p'),
    device ENUM('mobile', 'desktop'),
    PRIMARY KEY (sid),
    FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
    FOREIGN KEY (rid, ep_num) REFERENCES Videos(rid, ep_num) ON DELETE CASCADE)"""

    reviews = """CREATE TABLE Reviews (
    rvid INT,
    uid INT NOT NULL,
    rid INT NOT NULL,
    rating DECIMAL(2, 1) NOT NULL CHECK (rating BETWEEN 0 AND 5),
    body TEXT,
    posted_at DATETIME NOT NULL,
    PRIMARY KEY (rvid),
    FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
    FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE)"""

    cursor.execute(user)
    cursor.execute(producers)
    cursor.execute(viewers)
    cursor.execute(releases)
    cursor.execute(movies)
    cursor.execute(series)
    cursor.execute(videos)
    cursor.execute(sessions)
    cursor.execute(reviews)

def parsingfiles(cursor, connection, sysargv):
    try:
        files = [
            ("users.csv", 'Users', "INSERT INTO Users (uid, email, joined_date, nickname, street, city, state, zip, genres) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"),
            ("producers.csv", 'Producers', "INSERT INTO Producers (uid, bio, company) VALUES (%s, %s, %s)"),
            ("viewers.csv", 'Viewers', "INSERT INTO Viewers (uid, subscription, first_name, last_name) VALUES (%s, %s, %s, %s)"),
            ("releases.csv", 'Releases', "INSERT INTO Releases (rid, producer_uid, title, genre, release_date) VALUES (%s, %s, %s, %s, %s)"),
            ("movies.csv", 'Movies', "INSERT INTO Movies (rid, website_url) VALUES (%s, %s)"),
            ("series.csv", 'Series', "INSERT INTO Series (rid, introduction) VALUES (%s, %s)"),
            ("videos.csv", 'Videos', "INSERT INTO Videos (rid, ep_num, title, length) VALUES (%s, %s, %s, %s)"),
            ("sessions.csv", 'Sessions', "INSERT INTO Sessions (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"),
            ("reviews.csv", 'Reviews', "INSERT INTO Reviews (rvid, uid, rid, rating, body, posted_at) VALUES (%s, %s, %s, %s, %s, %s)")
        ]

        for filename, table_name, query in files:
            with open(f'{sysargv}/{filename}', 'r') as file:
                reader = csv.reader(file)
                next(reader)  # Skip header row
                for row in reader:
                    cursor.execute(query, row)
                    connection.commit()

        print("Success\n")

    except Exception as e:
        print("Fail\n ")
        print(f"Error: {e}")

#can someone check me to see if these inserts goes anywhere else based on the ER diagram...
        # missing one part of the insert viewer2 test but idk what to fix

def insertions(cursor, connection, x, insert_value):
    try:

        if insert_value == 'insertViewer':
            cursor.execute('INSERT INTO Viewers (uid, subscription, first_name, last_name) VALUES (%s, %s, %s, %s)', (int(x[1])), x[2], x[3], x[4])
            cursor.execute('INSERT INTO Users (uid, email, joined_date, nickname, street, city, state, zip, genres) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', (x[0]), x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11])
            cursor.execute('INSERT INTO Producers (uid, bio, company) VALUES (%s, %s, %s)', (int(x[1]), x[12], x[13]))
            cursor.execute('INSERT INTO Sessions (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', (x[14]), (x[15]), (x[16]), (x[17]), x[18], x[19], x[20], x[21])
            cursor.execute('INSERT INTO Reviews (uid, rid, rating, body, posted_at) VALUES (%s, %s, %s, %s, %s)', (int(x[1]), int(x[14]), float(x[15]), x[16], x[17]))
            connection.commit()
        print("Success\n")

        if insert_value == 'insertMovie':
            cursor.execute('INSERT INTO Movies (rid, website_url) VALUES (%s, %s)', (int(x[0]), x[1]))
            connection.commit()
            print("Success\n")

        if insert_value == 'insertSession':
            cursor.execute('INSERT INTO sessions (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', (x[0]), (x[1]), (x[2]), (x[3]), x[4], x[5], x[6], x[7])
            connection.commit()
            print('Success\n')

    except mysql.connector.Error as e:
        print(f'Fail\n')


def deletions(cursor, connection, x, delete_value):
    try:
        if delete_value == 'deleteViewer':
            cursor.execute('DELETE FROM Viewers WHERE uid = %s', (int(x[1]),))
            cursor.execute('DELETE FROM Users WHERE uid = %s', (int(x[1]),))
            connection.commit()
        print("Success\n")

    except mysql.connector.Error as e:
        print(f'Fail\n')


## missing one part of the addGenre2 test but idk what to fix 
def addGenre(cursor, connection, uid, new_genre):
    try:
        cursor.execute(""" UPDATE Releases SET genre = CONCAT(genre, ';', %s) WHERE rid = %s """, (new_genre, uid))
        connection.commit()
        print("Success\n")
    
    except mysql.connector.Error:
        print("Fail\n")



def updating(cursor, connection, x):
    try:
        if x[0] != "updateRelease": 
            print("Fail\n")
            return False
        rid = int(x[1]) 
        title = ' '.join(x[2:]) 

        cursor.execute('UPDATE Releases SET Title = %s WHERE rid = %s', (title, rid))
        connection.commit()

        print("Success\n")
        return True  

    except mysql.connector.Error as e:
        print(f"Fail\n")
        return False
    
#8 i dont think it works LOL
def releasereview(cursor, connection, uid):
    
    query = """SELECT DISTINCT r.rid, r.genre, r.title 
                          FROM Reviews rv 
                          JOIN Releases r ON rv.rid = r.rid 
                          WHERE rv.uid = %s 
                          ORDER BY r.title ASC"""

    cursor.execute(query, (uid,))
    all_data = cursor.fetchall()
    
    return all_data 

#works now i think
def popular(cursor, connection, N):
    query = """
        SELECT r.rid, r.title, COUNT(rv.rid) AS review_count 
        FROM Releases r 
        JOIN Reviews rv ON r.rid = rv.rid 
        GROUP BY r.rid, r.title 
        ORDER BY review_count DESC, r.rid ASC 
        LIMIT %s
    """

    cursor.execute(query, (N,))
    all_data = cursor.fetchall()

    for row in all_data:
        print(f"{row[0]},{row[1]},{row[2]}") 
    
    return all_data  

        

# #number 10 lol error oopsies 
# def releaseTitle(cursor, conn, sysargv): 
#     try:
#         cursor.execute("""
#             SELECT r.ReleaseID, r.Title AS release_title, r.Genre, v.Title AS video_title, v.EpisodeNumber, v.Length
#             FROM Sessions s
#             INNER JOIN Videos v ON s.VideoID = v.VideoID
#             INNER JOIN Releases r ON v.ReleaseID = r.ReleaseID
#             WHERE s.SessionID = %s
#             ORDER BY r.Title ASC
#             GROUP BY r.ReleaseID, r.Title;""", (sysargv[2],))

#         all_data = cursor.fetchall()
#         output_list = [x for x in all_data]

#         for x in output_list:
#             print(f"{x[0]},{x[1]},{x[2]},{x[3]},{x[4]},{x[5]}")

#     except mysql.connector.Error:
#         print("Fail\n")

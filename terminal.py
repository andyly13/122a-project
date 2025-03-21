import csv
import sys
import os
import mysql.connector
from mysql.connector.errors import Error

#make the connection here
def connect(User, Password, DBName):
    try:
        connection = mysql.connector.connect(user=User, password=Password, database=DBName)
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

    users = """CREATE TABLE Users (
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
    rating DECIMAL(2, 1) NOT NULL,
    body TEXT,
    posted_at DATETIME NOT NULL,
    PRIMARY KEY (rvid),
    FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
    FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE)"""

    cursor.execute(users)
    cursor.execute(producers)
    cursor.execute(viewers)
    cursor.execute(releases)
    cursor.execute(movies)
    cursor.execute(series)
    cursor.execute(videos)
    cursor.execute(sessions)
    cursor.execute(reviews)

def parsingfiles(cursor, connection, sysargv):
    #insert all the files here, be careful of order
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
                #skip the first line
                next(reader)
                for row in reader:
                    cursor.execute(query, row)
                    connection.commit()

        print("Success\n")

    except Exception as e:
        print("Fail\n ")

#insert viewer, movie, and session here
def insertions(cursor, connection, insert_value, x):
    try:
        if insert_value == 'insertViewer':
            cursor.execute("SELECT COUNT(*) FROM Users WHERE uid = %s", (int(x[0]),))
            if cursor.fetchone()[0] > 0:
                print("Fail\n")
                return
            cursor.execute('INSERT INTO Users (uid, email, joined_date, nickname, street, city, state, zip, genres) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', (int(x[0]), x[1], x[8], x[2], x[3], x[4], x[5], x[6], x[7]))
            cursor.execute('INSERT INTO Viewers (uid, subscription, first_name, last_name) VALUES (%s, %s, %s, %s)', (int(x[0]), x[11], x[9], x[10]))
            connection.commit()
            print("Success\n")

        if insert_value == 'insertMovie':
            cursor.execute("SELECT COUNT(*) FROM Movies WHERE rid = %s", (int(x[0]),))
            if cursor.fetchone()[0] > 0:
                print("Fail\n")
                return
            cursor.execute('INSERT INTO Movies (rid, website_url) VALUES (%s, %s)', (int(x[0]), x[1]))
            connection.commit()
            print("Success\n")

        if insert_value == 'insertSession':
            cursor.execute("SELECT COUNT(*) FROM Sessions WHERE sid = %s", (int(x[0]),))
            if cursor.fetchone()[0] > 0:
                print("Fail\n")
                return
            cursor.execute('INSERT INTO Sessions (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', (int(x[0]), (x[1]), (x[2]), (x[3]), x[4], x[5], x[6], x[7]))
            connection.commit()
            print('Success\n')
    
    except mysql.connector.Error as e:
        print(f'Fail\n')

#delete here
def deletions(cursor, connection, uid):
    uid = sys.argv[2]
    try:
        
        cursor.execute('DELETE FROM Viewers WHERE uid = %s', (uid,))
        connection.commit()
        print("Success\n")

    except mysql.connector.Error as e:
        print(f'Fail\n')


#both genre1 + 2 work now :)
def addGenre(cursor, connection, uid, new_genre):

    try:
        cursor.execute("SELECT genres FROM Users WHERE uid = %s", (uid,))
        result = cursor.fetchone()

        if not result: 
            print("Fail\n")
            return False

    #this part checks the null values 
        current = result[0] or "" 
        genre_list = set(current.lower().split(';')) if current else set()

        if new_genre.lower() in genre_list:
            print("Fail\n")
            return False

        updated = ";".join(genre_list | {new_genre})
        cursor.execute("UPDATE Users SET genres = %s WHERE uid = %s", (updated, uid))
        connection.commit()

        print("Success\n")
        return True

    except mysql.connector.Error:
        print("Fail\n")
        return False

#update here
def updating(cursor, connection, x):
    try:
        rid = int(x[1]) 
        title = ' '.join(x[2:]) 

        cursor.execute('UPDATE Releases SET title = %s WHERE rid = %s', (title, rid))
        connection.commit()

        print("Success\n")
        return True  

    except mysql.connector.Error as e:
        print(f"Fail\n")
        return False
    
#8
def releasereview(cursor, connection, uid):

    query = """SELECT DISTINCT r.rid, r.genre, r.title
    FROM Viewers AS v
    JOIN Reviews AS rev ON rev.uid = v.uid
    JOIN Releases AS r ON rev.rid = r.rid
    WHERE v.uid = %s
    ORDER BY r.title ASC""" 

    cursor.execute(query, (uid,))
    results = cursor.fetchall()

    for x in results:
        print(f"{x[0]},{x[1]},{x[2]}")

#9
def popular(cursor, connection, N):
    query = """
        SELECT r.rid, r.title, COUNT(rv.rid) AS revCount 
        FROM Releases r 
        JOIN Reviews rv ON r.rid = rv.rid 
        GROUP BY r.rid, r.title 
        ORDER BY revCount DESC, r.rid DESC 
        LIMIT %s"""

    cursor.execute(query, (N,))
    all_data = cursor.fetchall()

    for x in all_data:
        print(f"{x[0]},{x[1]},{x[2]}") 
    
    return all_data  
        

# #number 10
def releaseTitle(cursor, conn, sid): 
    try:
        query = """
            SELECT r.rid, r.title AS release_title, r.genre, v.title AS video_title, v.ep_num, v.length
            FROM Sessions s
            JOIN Videos v ON s.rid = v.rid AND s.ep_num = v.ep_num
            JOIN Releases r ON v.rid = r.rid
            WHERE s.sid = %s
            ORDER BY r.title ASC """
        
        cursor.execute(query, (sid,))
        results = cursor.fetchall()
        #print the table here
        for x in results:
            print(f"{x[0]},{x[1]},{x[2]},{x[3]},{x[4]},{x[5]}")

    except mysql.connector.Error:
        print("Fail\n")


#number 11

def activeViewer(cursor, con, n, start_date, end_date):
    query = """SELECT v.uid, v.first_name, v.last_name 
        FROM Viewers v
        JOIN Sessions s ON v.uid = s.uid
        WHERE s.initiate_at >= %s AND s.initiate_at <= %s
        GROUP BY v.uid, v.first_name, v.last_name
        HAVING COUNT(*) >= %s
        ORDER BY v.uid ASC"""
    
    cursor.execute(query, (start_date, end_date, n))
    results = cursor.fetchall()
    for x in results:
        print(f"{x[0]},{x[1]},{x[2]}")


#12
def videosViewed(cursor, connection, rid):
    query = """SELECT v.rid, v.ep_num, v.title, v.length, COUNT(DISTINCT s.uid) AS COUNT 
            FROM Videos v
            LEFT JOIN Sessions AS s ON v.rid = s.rid
            WHERE v.rid = %s
            GROUP BY v.rid, v.ep_num, v.title, v.length
            ORDER BY v.rid """
    
    cursor.execute(query, (rid,))
    results = cursor.fetchall()
    
    for x in results:
        print(f"{x[0]},{x[1]},{x[2]},{x[3]},{x[4]}")


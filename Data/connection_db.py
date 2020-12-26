import sqlite3

conn = sqlite3.connect('Biathlon_Data.db')

print("Opened database successfully")

conn.execute('''CREATE TABLE ATHLETES
         (ID INT PRIMARY KEY     NOT NULL,
         NAME           TEXT    NOT NULL,
         AGE            INT     NOT NULL,
         ADDRESS        CHAR(50),
         SALARY         REAL);''')
print("Table created successfully");

conn.close()
import psycopg2

# setting up

# define parameters
host = "pagila-db.cer332r9isea.us-east-1.rds.amazonaws.com"
dbname = "pagila"
user = "postgres"
password = "mypassword"

try:
    conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password}")
    cur = conn.cursor()
except Exception as e:
    print("Couldn't connect to the database")
    print(e)

# set autocommit
conn.set_session(autocommit=True)

# drop fact and dimension tables

try: 
    cur.execute("DROP TABLE dimFilm CASCADE;")
except psycopg2.Error as e: 
    print("Error: Unable to drop table")
    print (e)

try: 
    cur.execute("DROP TABLE dimDate CASCADE;")
except psycopg2.Error as e: 
    print("Error: Unable to drop table")
    print (e)

try: 
    cur.execute("DROP TABLE dimCustomerLocation CASCADE;")
except psycopg2.Error as e: 
    print("Error: Unable to drop table")
    print (e)

try: 
    cur.execute("DROP TABLE factSales CASCADE;")
except psycopg2.Error as e: 
    print("Error: Unable to drop table")
    print (e)

cur.close()
conn.close()
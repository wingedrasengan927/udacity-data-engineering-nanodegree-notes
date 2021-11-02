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

# create fact and dimension tables

# create dimension table film
# we cannot use film_id as the primary key
# as it will contradict with the table film
query = """
CREATE TABLE dimFilm
(
  film_key            SERIAL PRIMARY KEY,
  title              varchar(255) NOT NULL
);
"""
try: 
    cur.execute(query)
except psycopg2.Error as e: 
    print("Error: Unable to create table")
    print (e)

# create dimension table date
query = """
CREATE TABLE dimDate
(
    date_key varchar(50) PRIMARY KEY,
    purchase_time timestamp NOT NULL,
    date date     NOT NULL,
    year smallint NOT NULL,
    month smallint NOT NULL
);
"""
try: 
    cur.execute(query)
except psycopg2.Error as e: 
    print("Error: Unable to create table")
    print (e)

# create dimension table customerLocation
query = """
CREATE TABLE dimCustomerLocation
(
    customer_key SERIAL PRIMARY KEY,
    city        varchar(255) NOT NULL,
    country     varchar(255) NOT NULL
);
"""
try: 
    cur.execute(query)
except psycopg2.Error as e: 
    print("Error: Unable to create table")
    print (e)

# create facts table
query = """
CREATE TABLE factSales
(
    sales_key SERIAL PRIMARY KEY,
    date_key varchar(50) REFERENCES dimDate(date_key),
    customer_key integer REFERENCES dimCustomerLocation(customer_key),
    film_key integer REFERENCES dimFilm(film_key),
    sales_amount numeric NOT NULL 
);
"""
try: 
    cur.execute(query)
except psycopg2.Error as e: 
    print("Error: Unable to create table")
    print (e)

cur.close()
conn.close()
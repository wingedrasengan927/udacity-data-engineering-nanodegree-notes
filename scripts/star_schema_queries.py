import psycopg2
import pandas as pd
import pandas.io.sql as sqlio

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

# question: Get the total amount spent in each year, each month by each country and each city
# we need to join tables as follows:
# factSales -> dimCustomerLocation
#           -> dimDate
query = """
SELECT year, month, country, city,
SUM(sales_amount) as revenue 
FROM factSales fs 
JOIN dimDate dd ON fs.date_key = dd.date_key  
JOIN dimCustomerLocation dcu ON fs.customer_key = dcu.customer_key
GROUP BY year, month, country, city 
ORDER BY revenue DESC
LIMIT 100; 
"""
df = sqlio.read_sql_query(query, conn)
print(df)

print("\n")

# Question: Get number of times a film has been rented in each year and each month
# we need to join tables as follows:
# factSales -> dimFilm
#           -> dimDate
query = """
SELECT title, year, month,
COUNT(title) as count 
FROM factSales fs 
JOIN dimDate dd ON fs.date_key = dd.date_key  
JOIN dimFilm df  ON fs.film_key = df.film_key
GROUP BY title, year, month
ORDER BY count DESC; 
"""
df = sqlio.read_sql_query(query, conn)
print(df)

print("\n")

# Question: Get number of times a film has been rented by each country and each city
# we need to join tables as follows:
# factSales -> dimFilm 
#           -> dimCustomerLocation
query = """
SELECT title, country, city, 
COUNT(title) as count
FROM factSales fs 
JOIN dimFilm df
ON fs.film_key = df.film_key 
JOIN dimCustomerLocation dcu 
ON fs.customer_key = dcu.customer_key 
GROUP BY title, country, city 
ORDER BY count DESC
LIMIT 100;
"""
df = sqlio.read_sql_query(query, conn)
print(df)

# We observe that:
# 1. the results are same
# 2. queries are easier to write in star schema
# 3. there is a huge performance improvement in star schema

cur.close()
conn.close()
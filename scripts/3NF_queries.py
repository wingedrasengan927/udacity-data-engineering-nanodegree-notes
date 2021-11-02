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
# country -> city -> address -> customer -> payment

query = """
SELECT EXTRACT(year FROM payment_date) as year, EXTRACT(month FROM payment_date) as month, 
country, city, SUM(amount) as amount 
FROM country co JOIN city ci 
ON co.country_id = ci.country_id 
JOIN address ad 
ON ci.city_id = ad.city_id 
JOIN customer cu 
ON ad.address_id = cu.address_id 
JOIN payment p 
ON cu.customer_id = p.customer_id 
GROUP BY year, month, country, city 
ORDER BY amount DESC
LIMIT 100;
"""

# execute query and load result into a pandas dataframe
df = sqlio.read_sql_query(query, conn)
print(df)

print("\n")

# Question: Get number of times a film has been rented in each year and each month
# we need to join tables as follows:
# film -> inventory -> rental -> payment
query = """
SELECT title, EXTRACT(year FROM payment_date) as year, EXTRACT(month FROM payment_date) as month, 
COUNT(title) as count
FROM film f JOIN inventory i 
ON f.film_id = i.film_id 
JOIN rental r 
ON i.inventory_id = r.inventory_id 
JOIN payment p 
ON r.rental_id = p.rental_id 
GROUP BY title, year, month 
ORDER BY count DESC
LIMIT 100;
"""
df = sqlio.read_sql_query(query, conn)
print(df)

print("\n")

# Question: Get number of times a film has been rented by each country and each city
# we need to join tables as follows:
# film -> inventory -> rental -> customer -> address -> city -> country
query = """
SELECT title, country, city, 
COUNT(title) as count
FROM film f JOIN inventory i 
ON f.film_id = i.film_id 
JOIN rental r 
ON i.inventory_id = r.inventory_id 
JOIN customer cu 
ON r.customer_id = cu.customer_id 
JOIN address ad 
ON cu.address_id = ad.address_id 
JOIN city ci 
ON ad.city_id = ci.city_id 
JOIN country co 
ON ci.country_id = co.country_id
GROUP BY title, country, city 
ORDER BY count DESC
LIMIT 100;
"""
df = sqlio.read_sql_query(query, conn)
print(df)

cur.close()
conn.close()
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

# Insert data into fact and dimension tables

# dimFilm
query = """
INSERT INTO dimFilm (film_key, title)
SELECT  film_id AS film_key,
        title
FROM film;
"""
try: 
    cur.execute(query)
except psycopg2.Error as e: 
    print("Error: Unable to insert data")
    print (e)

# dimDate
# what should be the primary key for dimDate
# we can convert the timestamp to integer like 2006-12-23 06:04:37.491 to 20061223060437491
# however this would occupy a lot of storage
# so we can convert to varchar and use it
# 
query = """
INSERT INTO dimDate (date_key, purchase_time, date, year, month)
SELECT  TO_CHAR(payment_date :: TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.MS') AS date_key,
        payment_date AS purchase_time,
        date(payment_date) AS date,
        EXTRACT(year FROM payment_date) AS year,
        EXTRACT(month FROM payment_date) AS month
FROM payment;
"""
try: 
    cur.execute(query)
except psycopg2.Error as e: 
    print("Error: Unable to insert data")
    print (e)

# dimCustomerLocation
query = """
INSERT INTO dimCustomerLocation (customer_key, city, country)
SELECT  cu.customer_id AS customer_key, 
        ci.city,
        co.country
FROM customer cu
JOIN address a ON (cu.address_id = a.address_id)
JOIN city ci ON (a.city_id = ci.city_id)
JOIN country co ON (ci.country_id = co.country_id);
"""
try: 
    cur.execute(query)
except psycopg2.Error as e: 
    print("Error: Unable to insert data")
    print (e)

# fact Sales
query = """
INSERT INTO factSales (date_key, customer_key, film_key, sales_amount)
SELECT  TO_CHAR(p.payment_date :: TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.MS') AS date_key,
        p.customer_id AS customer_key,
        i.film_id AS film_key,
        p.amount AS sales_amount
FROM payment p
JOIN rental r ON (p.rental_id = r.rental_id)
JOIN inventory i ON (r.inventory_id = i.inventory_id);
"""
try: 
    cur.execute(query)
except psycopg2.Error as e: 
    print("Error: Unable to insert data")
    print (e)

cur.close()
conn.close()
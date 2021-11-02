
## Setting Up

### Install PostgreSQL on Ubuntu
**Reference Article:** https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-18-04

**Note**: Check the version of PostgreSQL you have installed in the instance and make sure it matches with the version of the RDS Instance.

### Connect to PostgreSQL RDS Instance
1. Switch user: `sudo -i -u [username]` 
2. `username` would be `postgres` by default.
3. Connect to the database: `psql -d [database_name] -h [instance_endpoint] -p [port] -U [username]`

## Dimensional Modelling

**Reference Article:** https://www.guru99.com/dimensional-model-data-warehouse.html
Business Questions
Get the total amount spent in each year, each month by each country and each city
Get number of times a film has been rented in each year and each month
Get number of times a film has been rented by each country and each city

we observe that to answer the above business questions, we need the following dimensions:
1. film (product) 2. location 3. time

and we need the following attributes in each dimension
film: film_id, film_title
location: customer_id, city, country
time: year, month

fact table
payment_id


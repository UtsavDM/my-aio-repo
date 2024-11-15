-- 1. DISTINCT Districts the customers are from:
SELECT DISTINCT district FROM address;

-- 2. latest rental date:
SELECT rental_date FROM rental ORDER BY rental_date DESC LIMIT 1;
SELECT MAX(rental_date) FROM rental;

-- 3. How many films does the company have:
SELECT COUNT(*) FROM film;

-- 4. How many distinct last names of the customers are there:
SELECT COUNT(DISTINCT last_name) FROM customer;

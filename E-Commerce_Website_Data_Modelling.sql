-- Filtering the world data to Cities Located in Brazil:
CREATE VIEW brazil_cities
AS
  SELECT id                AS city_id,
         Lower(city_ascii) AS city,
         lat               AS latitude,
         lng               AS longitude
  FROM   my_database.dbo.world_cities
  WHERE  country = 'Brazil';

-- Modelling the customers table:
CREATE VIEW customers
AS
  SELECT DISTINCT customer_id,
                  city_id,
                  geolocation_zip_code_prefix AS customer_zip_code,
                  geolocation_state           AS state
  FROM   (SELECT *
          FROM   [E-Commerce Database].dbo.customers_dataset
          WHERE  geolocation_city IN (SELECT DISTINCT city
                                      FROM
                 [E-Commerce Database].dbo.brazil_cities)) A
         JOIN brazil_cities B
           ON A.geolocation_city = B.city;

-- Modelling the sellers table:
CREATE VIEW sellers
AS
  SELECT DISTINCT seller_id,
                  city_id,
                  geolocation_zip_code_prefix AS seller_zip_code,
                  geolocation_state           AS state
  FROM   (SELECT *
          FROM   [E-Commerce Database].dbo.sellers_dataset
          WHERE  geolocation_city IN (SELECT DISTINCT city
                                      FROM
                 [E-Commerce Database].dbo.brazil_cities)) A
         JOIN brazil_cities B
           ON A.geolocation_city = B.city;

-- Modelling data to create a locations column:
select * from [E-Commerce Database]..cities
CREATE VIEW cities
AS
  WITH a
       AS (SELECT DISTINCT city_id,
                           state
           FROM   sellers
           UNION ALL
           SELECT DISTINCT city_id,
                           state
           FROM   customers)
  SELECT DISTINCT a_2.city_id,
                  city,
                  latitude,
                  longitude,
                  region,
                  state,
                  [state name]                                           AS
                  state_name,
                  [population in thousands (2017 estimate in thousands)] AS
                  state_population,
                  [per capita gnp in reais (r$)]                         AS
                  'State_GNP_in_Reais_(R$)'
  FROM   (SELECT *
          FROM   a) a_2
         JOIN brazil_cities b
           ON a_2.city_id = b.city_id
         JOIN brazil_states_2017$ bs
           ON bs.[state abb] = a_2.state
		  

-- Modelling the review dataset:
CREATE VIEW grouped_reviews
AS
  SELECT order_id,
         Avg(review_score) AS average_review_score,
         Count(review_id)  AS number_of_reviews
  FROM   reviews_dataset
  GROUP  BY order_id;

-- Modelling the data to create a single fact table:
-- Fetching only the geological data needed for the analysis which are the orders happened in 2017:
CREATE VIEW analysis_period_orders
AS
  SELECT *
  FROM   orders_dataset
  WHERE  order_purchase_timestamp BETWEEN '2017-01-01' AND '2018-01-01';

-- Creating a view for the orders without the dates:
CREATE VIEW analysis_period_full_orders
AS
  SELECT A.order_id,
         customer_id,
         product_id,
         seller_id,
         price,
         freight_value,
         payment_type,
         payment_sequential,
         payment_installments,
         payment_value,
         order_purchase_timestamp,
         order_approved_at,
         order_delivered_carrier_date,
         order_delivered_customer_date,
         order_estimated_delivery_date,
         shipping_limit_date,
         average_review_score,
         number_of_reviews
  FROM   analysis_period_orders A
         JOIN items_dataset I
           ON A.order_id = I.order_id
         JOIN grouped_reviews R
           ON A.order_id = R.order_id
         JOIN payments_dataset P
           ON A.order_id = P.order_id

-- Creating a view for the orders analysis:
CREATE VIEW full_orders_of_2017
AS
  SELECT *,
         Datediff(minute, order_purchase_timestamp, order_approved_at)
            AS
         order_approval_time_min,
         Datediff(day, order_purchase_timestamp, order_approved_at)
            AS
         order_approval_time_days,
         Datediff(minute, order_approved_at, order_delivered_carrier_date)
            AS
         order_delivery_time_to_carrier_min,
         Datediff(day, order_approved_at, order_delivered_carrier_date)
            AS
         order_delivery_time_to_carrier_days,
         Datediff(minute, order_delivered_carrier_date,
         order_delivered_customer_date) AS
         shipping_time_min,
         Datediff(day, order_delivered_carrier_date,
         order_delivered_customer_date
            )    AS
         shipping_time_days,
         Datediff(minute, order_purchase_timestamp,
         order_delivered_customer_date)
            AS
         total_order_delivery_time_min,
         Datediff(day, order_purchase_timestamp, order_delivered_customer_date)
            AS
         total_order_delivery_time_days,
         CASE
           WHEN Datediff(day, order_delivered_customer_date,
                order_estimated_delivery_date) >= 0 THEN 'In-time'
           ELSE 'Late'
         END
            AS order_delivery_status,
         CASE
           WHEN Datediff(minute, order_delivered_customer_date,
                shipping_limit_date) >= 0
         THEN 'Within Limit Range'
           ELSE 'Exceeded Limit Range'
         END
            AS shipping_limit_status
  FROM   analysis_period_full_orders A

-- Categorizing Products to give additional analytical prospective:
CREATE VIEW full_products
AS
  SELECT *,
         CASE
           WHEN product_name_lenght < 10 THEN 'Less than 10 words'
           WHEN product_description_lenght < 50 THEN '10 to 50 words'
           ELSE 'More than 50 words'
         END
            AS product_name_length_category,
         CASE
           WHEN product_description_lenght < 100 THEN 'Less than 100 words'
           WHEN product_description_lenght < 500 THEN '100 to 500 words'
           WHEN product_description_lenght < 1000 THEN '500 to 1000 words'
           WHEN product_description_lenght < 2000 THEN '1000 to 2000 words'
           ELSE 'More than 2000 words'
         END
            AS product_description_length_category,
         CASE
           WHEN product_photos_qty = 1 THEN 'One Photo'
           WHEN product_photos_qty = 2 THEN 'Two Photos'
           WHEN product_photos_qty = 3 THEN 'Three Photos'
           WHEN product_photos_qty >= 4 THEN 'More than 4 Photos'
         END
            AS product_photos_category,
         Concat(product_length_cm, ' x ', product_height_cm, ' x ',
         product_width_cm) AS
         'product_dimensions_cm',
         CASE
           WHEN ( product_weight_g / 1000 ) < 1 THEN 'Less than 1kg'
           WHEN ( product_weight_g / 1000 ) < 5 THEN '1kg to 5kg'
           WHEN ( product_weight_g / 1000 ) < 10 THEN '5kg to 10kg'
           WHEN ( product_weight_g / 1000 ) < 20 THEN '10kg to 20kg'
           ELSE 'More than 20kg'
         END
            AS product_weight_category
  FROM   products_dataset; 
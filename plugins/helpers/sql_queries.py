class SqlQueries:
    user_dim_table_insert = """
     INSERT INTO user_dim(user_id, name, yelping_since, usefull, funny, cool, fans, avg_stars) 
        SELECT 
          user_id, name,  trunc(yelping_since), useful, funny, cool, fans, average_stars
          FROM staging_users
    """
    review_fact_table_insert = """
     INSERT INTO review_fact(review_id, user_id, business_id, stars, date, text, usefull, funny, cool) 
        SELECT 
          review_id, user_id, business_id, stars, date, text, useful, funny, cool
          FROM staging_reviews
    """
    business_dim_table_insert = """
     INSERT INTO business_dim(business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open)  
        SELECT  business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open
          FROM staging_businesses
    """

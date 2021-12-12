

class TableInsertQueries(object):
    __dict = {
        'dim_user': """
     INSERT INTO dim_user(user_id, name, yelping_since, usefull, funny, cool, fans, avg_stars) 
        SELECT 
          user_id, name,  trunc(yelping_since), useful, funny, cool, fans, average_stars
          FROM staging_users
    """,
        'fact_review': """
         INSERT INTO fact_review(review_id, user_id, business_id, stars, date, text, usefull, funny, cool)
            SELECT
              review_id, user_id, business_id, stars, date, text, useful, funny, cool
              FROM staging_reviews
        """,
        'dim_business': """
         INSERT INTO dim_business(business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open)
            SELECT  business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open
              FROM staging_businesses
        """
    }

    @staticmethod
    def get(table_name: str):
        return TableInsertQueries.__dict[table_name]




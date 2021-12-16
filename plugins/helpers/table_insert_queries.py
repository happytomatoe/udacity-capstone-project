# TODO: add date key to fact_reviews and dim_user
class TableInsertQueries(object):
    __dict = {
        'dim_user': """
         INSERT INTO dim_user(user_id, name, yelping_since, usefull, funny, cool, fans, avg_stars) 
            SELECT 
              user_id, name,  yelping_since::date, useful, funny, cool, fans, average_stars
         FROM staging_users
    """,
        'dim_business': """
            INSERT INTO dim_business(business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open)
               SELECT  business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open
                 FROM staging_businesses
           """,
        'fact_review': """
             INSERT INTO fact_review(review_id, user_id, business_id, stars, date, text, usefull, funny, cool)
                SELECT
                  review_id, user_id, business_id, stars, date, text, useful, funny, cool
                  FROM staging_reviews
            """,
        'fact_business_category': """
            INSERT INTO fact_business_category(business_id, category)
            -- https://stackoverflow.com/questions/22643338/sequence-number-generation-function-in-aws-redshift
            with seq_0_9 as (
            select 0 as num
            union all
            select 1 as num
            union all select 2 as num
            union all select 3 as num
            union all select 4 as num
            union all select 5 as num
            union all select 6 as num
            union all select 7 as num
            union all select 8 as num
            union all select 9 as num
            ), seq_1_99 AS (
               select a.num + b.num * 10 as num
                from seq_0_9 a, seq_0_9 b
                order by num offset  1
            )
            select st_b.business_id, TRIM(SPLIT_PART(st_b.categories, ',', seq.num))
            from  seq_1_99 as seq
            inner join staging_businesses st_b ON seq.num <= REGEXP_COUNT(st_b.categories, ',') + 1
            """,
        'fact_checkin': """
        INSERT INTO fact_checkin(business_id, timestamp) 
        SELECT business_id, date FROM staging_checkins
        """,
        'fact_tip': """
            INSERT INTO fact_tip(user_id, business_id, text, compliment_count)  
            SELECT user_id, business_id, text, compliment_count  FROM  staging_tips
            """,

    }

    @staticmethod
    def get(table_name: str):
        return TableInsertQueries.__dict[table_name]

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
        'dim_date_time': """
            INSERT INTO dim_date(date_key, date, day, week, weekday, month, year)
            SELECT DISTINCT date_part('year', yelping_since) * 10000 +
                            date_part('month', yelping_since) * 100 +
                            date_part('day', yelping_since),
                            trunc(yelping_since),
                            extract(day from yelping_since),
                            extract(week from yelping_since),
                            extract(weekday from yelping_since),
                            extract(month from yelping_since),
                            extract(year from yelping_since)
            FROM staging_users
            UNION
            SELECT date_part('year', date) * 10000 +
                   date_part('month', date) * 100 +
                   date_part('day', date),
                   trunc(date),
                   extract(day from date),
                   extract(week from date),
                   extract(weekday from date),
                   extract(month from date),
                   extract(year from date)
            From staging_reviews
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
            ), seq_1_9999 AS (
                select a.num + b.num * 10 + c.num * 100 + d.num * 1000 as num
                from seq_0_9 a, seq_0_9 b, seq_0_9 c, seq_0_9 d
                order by num offset  1
            )
            select st_c.business_id, TRIM(SPLIT_PART(st_c.date, ',', seq.num))::timestamp
            from  seq_1_9999 as seq
            inner join staging_checkins st_c ON seq.num <= REGEXP_COUNT(st_c.date, ',') + 1
        """
    }

    @staticmethod
    def get(table_name: str):
        return TableInsertQueries.__dict[table_name]

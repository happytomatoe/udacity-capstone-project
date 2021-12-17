----------------------------------------------------- ***DIMENSIONS*** ---


CREATE TABLE IF NOT EXISTS "dim_date"
(
    date_id int4 PRIMARY KEY,
    date    date,
    day     int2,
    week    int2,
    weekday int2,
    month   int2,
    quarter int2,
    year    int2
);

-- Date Key (PK)
-- Date
-- Full Date Description
-- Day of Week
-- Day Number in Calendar Month
-- Day Number in Calendar Year
-- Calendar Week Ending Date
-- Calendar Week Number in Year
-- Calendar Month Name
-- Calendar Month Number in Year
-- Calendar Year-Month (YYYY-MM)
-- Calendar Quarter
-- Calendar Year-Quarter
-- Calendar Year
-- Holiday Indicator
-- Weekday Indicator
-- SQL Date Stamp


CREATE TABLE IF NOT EXISTS "dim_user"
(
    "user_id"               char(22) PRIMARY KEY,
    "name"                  text NOT NULL,
    "yelping_since"         timestamp,
    "yelping_since_date_id" int4 references dim_date,
--   TODO: do we need review_count?
    "usefull"               int4,
    "funny"                 int4,
    "cool"                  int4,
    "fans"                  int4,
    "avg_stars"             int4
);

-- FIXME: lat, lon converted to int
CREATE TABLE IF NOT EXISTS "dim_business"
(
    "business_id"  char(22) PRIMARY KEY,
    "name"         text NOT NULL,
    "address"      text,
    "city"         text,
--   TODO: check if there are many 3 char states
    "state"        varchar(3),
    "postal_code"  varchar(32),
    "latitude"     double precision,
    "longitude"    double precision,
    "stars"        real,
    "review_count" int4,
    "is_open"      boolean
);


-- ------------------------------------------------------FACTS ---

-- TODO: add surogate keys?
-- TODO: add ids to fact tables?

CREATE TABLE IF NOT EXISTS "fact_review"
(
    "review_id"   char(22) PRIMARY KEY,
    "user_id"     char(22) references dim_user     NOT NULL,
    "business_id" char(22) references dim_business NOT NULL,
    "stars"       int2,
    "date_id"     int4 references dim_date         NOT NULL,
    "text"        varchar(5000)                    NOT NULL,
    "usefull"     int4,
    "funny"       int4,
    "cool"        int4
);


CREATE TABLE IF NOT EXISTS "fact_business_category"
(
    "category"    text                             NOT NULL,
    "business_id" char(22) references dim_business NOT NULL
);

-- tips
-- {"user_id":"sNVpZLDSlCudlXLsnJpg7A","business_id":"Wqetc51pFQzz04SXh_AORA","text":"So busy...","date":"2014-06-07 12:09:55","compliment_count":0}

CREATE TABLE IF NOT EXISTS fact_tip
(
    user_id          char(22) references dim_user,
    business_id      char(22) references dim_business,
    text             varchar(5000),
    compliment_count int2
);

-- checkins
-- {"business_id":"--0zrn43LEaB4jUWTQH_Bg","date":"2010-10-08 22:21:20, 2010-11-01 21:29:14, 2010-12-23 22:55:45,
-- 2011-04-08 17:14:59, 2011-04-11 21:28:45, 2011-04-26 16:42:25, 2011-05-20 19:30:57, 2011-05-24 20:02:21, 2011-08-29 19:01:31"}

CREATE TABLE IF NOT EXISTS "fact_checkin"
(
    "business_id" char(22) references dim_business NOT NULL,
    "timestamp"   timestamp,
    "date_id"     int4 references dim_date
);

------------------------------------------------------------ STAGING ---

CREATE TABLE IF NOT EXISTS staging_users
(
    user_id            char(22),
    average_stars      real,
    compliment_cool    int4,
    compliment_cute    int4,
    compliment_funny   int4,
    compliment_hot     int4,
    compliment_list    int4,
    compliment_more    int4,
    compliment_note    int4,
    compliment_photos  int4,
    compliment_plain   int4,
    compliment_profile int4,
    compliment_writer  int4,
    cool               int4,
    elite              text,
    fans               int4,
    friends            text,
    funny              int4,
    name               text,
    review_count       int4,
    useful             int4,
--     TODO: Do we need timestamp or should I use date or int YYYYMMDD?
    yelping_since      timestamp
);

CREATE TABLE IF NOT EXISTS staging_reviews
(
    review_id   char(22),
    user_id     char(22),
    business_id char(22),
    cool        int4,
    date        timestamp,
    funny       int4,
    stars       real,
    text        varchar(5000),
    useful      int4
);

CREATE TABLE IF NOT EXISTS staging_businesses
(
    business_id  char(22),
    address      text,
    categories   text,
    city         text,
    is_open      boolean,
--     TODO: check if need to change
    latitude     double precision,
    longitude    double precision,
    name         text,
    postal_code  varchar(32),
    review_count int4,
    stars        real,
    state        varchar(3)
);



-- checkins
-- {"business_id":"--0zrn43LEaB4jUWTQH_Bg","date":"2010-10-08 22:21:20, 2010-11-01 21:29:14, 2010-12-23 22:55:45,
-- 2011-04-08 17:14:59, 2011-04-11 21:28:45, 2011-04-26 16:42:25, 2011-05-20 19:30:57, 2011-05-24 20:02:21, 2011-08-29 19:01:31"}

CREATE TABLE IF NOT EXISTS staging_checkins
(
    business_id char(22),
    date        timestamp
);

-- tips
-- {"user_id":"sNVpZLDSlCudlXLsnJpg7A","
-- business_id":"Wqetc51pFQzz04SXh_AORA",
-- "text":"So busy...",
-- "date":"2014-06-07 12:09:55",
-- "compliment_count":0}

CREATE TABLE IF NOT EXISTS staging_tips
(
    business_id      char(22),
    user_id          char(22),
    date             timestamp,
    text             varchar(max),
    compliment_count int2
);



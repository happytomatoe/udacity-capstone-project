----------------------------------------------------- ***DIMENSIONS*** ---


CREATE TABLE IF NOT EXISTS "dim_date"
(
    date_id int4 PRIMARY KEY distkey sortkey,
    date    date,
    day     int2,
    week    int2,
    weekday int2,
    month   int2,
    quarter int2,
    year    int2
);

CREATE TABLE IF NOT EXISTS "dim_user"
(
    "user_id"               char(22) PRIMARY KEY distkey sortkey,
    "name"                  text NOT NULL,
    "yelping_since"         timestamp,
    "yelping_since_date_id" int4 references dim_date,
    review_count            int4,
    "usefull"               int4,
    "funny"                 int4,
    "cool"                  int4,
    "fans"                  int4,
    "avg_stars"             int4
);

CREATE TABLE IF NOT EXISTS "dim_business"
(
    "business_id"  char(22) PRIMARY KEY distkey sortkey,
    "name"         text NOT NULL,
    "address"      text,
    "city"         text,
--     TODO: what to do with 3 char wrong province?
    "state"        varchar(3),
    "postal_code"  varchar(32),
--     TODO: this dimension missing country
    "latitude"     double precision,
    "longitude"    double precision,
    "stars"        real,
    "review_count" int4,
    "is_open"      boolean
);
-- TODO: should I add interleaved sort key?


-- ------------------------------------------------------FACTS ---

CREATE TABLE IF NOT EXISTS "fact_review"
(
    "review_id"   char(22) PRIMARY KEY distkey,
    "user_id"     char(22) references dim_user     NOT NULL,
    "business_id" char(22) references dim_business NOT NULL,
    "stars"       int2                             NOT NULL,
    "date_id"     int4 references dim_date         NOT NULL,
    "text"        varchar(5000)                    NOT NULL,
    "usefull"     int4,
    "funny"       int4,
    "cool"        int4
) INTERLEAVED SORTKEY (review_id,user_id,business_id,date_id);


CREATE TABLE IF NOT EXISTS "fact_business_category"
(
    "category"    text                             NOT NULL,
    "business_id" char(22) references dim_business NOT NULL distkey
);
-- TODO: should I add interleaved sort key?

-- tips
-- {"user_id":"sNVpZLDSlCudlXLsnJpg7A","business_id":"Wqetc51pFQzz04SXh_AORA","text":"So busy...","date":"2014-06-07 12:09:55","compliment_count":0}

CREATE TABLE IF NOT EXISTS fact_tip
(
    user_id          char(22) references dim_user     NOT NULL,
    business_id      char(22) references dim_business NOT NULL,
    text             varchar(5000)                    NOT NULL,
    compliment_count int2
);

-- checkins
-- {"business_id":"--0zrn43LEaB4jUWTQH_Bg","date":"2010-10-08 22:21:20, 2010-11-01 21:29:14, 2010-12-23 22:55:45,
-- 2011-04-08 17:14:59, 2011-04-11 21:28:45, 2011-04-26 16:42:25, 2011-05-20 19:30:57, 2011-05-24 20:02:21, 2011-08-29 19:01:31"}

CREATE TABLE IF NOT EXISTS "fact_checkin"
(
    "business_id" char(22) references dim_business NOT NULL,
    "timestamp"   timestamp                        NOT NULL sortkey,
    "date_id"     int4 references dim_date
);


CREATE TABLE IF NOT EXISTS "fact_friend"
(
    "user_id"   char(22) references dim_user NOT NULL,
    "friend_id" char(22) references dim_user NOT NULL
);


------------------------------------------------------------ STAGING ---

CREATE TABLE IF NOT EXISTS staging_users
(
    user_id       char(22),
    average_stars real,
    cool          int4,
--     elite              text,
    fans          int4,
--     TODO: do we need to add friends?
    friends       text,
    funny         int4,
    name          text,
    review_count  int4,
    useful        int4,
    yelping_since timestamp
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


CREATE TABLE IF NOT EXISTS staging_friends
(
    user_id   char(22),
    friend_id char(22)
);


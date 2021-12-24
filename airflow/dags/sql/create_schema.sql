----------------------------------------------------- DIMENSIONS ---


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

comment on column dim_date.date_id is 'date key';
comment on column dim_date.date is 'regular date';
comment on column dim_date.day is 'day of month';
comment on column dim_date.week is 'week number in the month';
comment on column dim_date.weekday is 'day of the week';
comment on column dim_date.weekday is 'month number. 0 - Monday, 6 - Sunday';
comment on column dim_date.quarter is 'quarter in a year';
comment on column dim_date.year is 'year number';



CREATE TABLE IF NOT EXISTS "dim_user"
(
    "user_id"               char(22) PRIMARY KEY distkey sortkey,
    "name"                  text NOT NULL,
    "yelping_since"         timestamp,
    "yelping_since_date_id" int4 references dim_date,
    "review_count"          int4,
    "usefull"               int4,
    "funny"                 int4,
    "cool"                  int4,
    "fans"                  int4,
    "avg_stars"             int4
);

comment on column dim_user.user_id is 'unique user id';
comment on column dim_user.name is 'the user''s first name';
comment on column dim_user.yelping_since is 'when the user joined Yelp';
comment on column dim_user.yelping_since_date_id is 'FK to date dimension';
comment on column dim_user.review_count is 'the number of reviews they''ve written';
comment on column dim_user.usefull is 'number of useful votes sent by the user';
comment on column dim_user.funny is 'number of funny votes sent by the user';
comment on column dim_user.cool is 'number of cool votes sent by the user';
comment on column dim_user.fans is 'number of fans the user has';
comment on column dim_user.avg_stars is 'average rating of all reviews';


CREATE TABLE IF NOT EXISTS "dim_business"
(
    "business_id"  char(22) PRIMARY KEY distkey sortkey,
    "name"         text NOT NULL,
    "address"      text,
    "city"         text,
    "state"        varchar(3),
    "postal_code"  varchar(32),
    "latitude"     double precision,
    "longitude"    double precision,
    "stars"        real,
    "review_count" int4,
    "is_open"      boolean
);

comment on column dim_business.business_id is 'unique business id';
comment on column dim_business.name is 'the business''s name';
comment on column dim_business.address is 'the full address of the business';
comment on column dim_business.city is 'the city';
comment on column dim_business.state is 'state code, if applicable';
comment on column dim_business.postal_code is 'the postal code';
comment on column dim_business.latitude is 'latitude';
comment on column dim_business.longitude is 'longitude';
comment on column dim_business.stars is 'star rating, rounded to half-stars';
comment on column dim_business.review_count is 'number of reviews';
comment on column dim_business.is_open is '0 or 1 for closed or open, respectively';


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

comment on column fact_review.review_id is 'unique review id';
comment on column fact_review.user_id is 'FK to the user dimension';
comment on column fact_review.business_id is 'FK to the business dimension';
comment on column fact_review.stars is 'star rating';
comment on column fact_review.date_id is 'FK to the date dimension';
comment on column fact_review.text is 'the review itself';
comment on column fact_review.usefull is 'number of useful votes received';
comment on column fact_review.funny is 'number of funny votes received';
comment on column fact_review.cool is 'number of cool votes received';


CREATE TABLE IF NOT EXISTS "fact_business_category"
(
    "category"    text                             NOT NULL,
    "business_id" char(22) references dim_business NOT NULL distkey sortkey
);

comment on column fact_business_category.category is 'business category';
comment on column fact_business_category.business_id is 'business id';


CREATE TABLE IF NOT EXISTS fact_tip
(
    user_id          char(22) references dim_user     NOT NULL,
    business_id      char(22) references dim_business NOT NULL,
    text             varchar(5000)                    NOT NULL,
    compliment_count int2
) INTERLEAVED SORTKEY (user_id, business_id);

comment on column fact_tip.user_id is 'FK to the user dimension';
comment on column fact_tip.business_id is 'FK to the business dimension';
comment on column fact_tip.text is 'text of the tip';
comment on column fact_tip.compliment_count is 'how many compliments the tip has';


CREATE TABLE IF NOT EXISTS "fact_checkin"
(
    "business_id" char(22) references dim_business NOT NULL,
    "timestamp"   timestamp                        NOT NULL,
    "date_id"     int4 references dim_date
);

comment on column fact_checkin.business_id is 'FK to the business dimension';
comment on column fact_checkin.timestamp is 'check-in time';
comment on column fact_checkin.date_id is 'date key which points to date based on truncating the timestamp';


CREATE TABLE IF NOT EXISTS "fact_friend"
(
    "user_id"   char(22) references dim_user NOT NULL,
    "friend_id" char(22) references dim_user NOT NULL
);

comment on column fact_friend.user_id is 'FK to the user dimension';
comment on column fact_friend.friend_id is 'FK to the user dimension which represents friend role';


------------------------------------------------------------ STAGING ---

CREATE TABLE IF NOT EXISTS staging_users
(
    user_id       char(22),
    average_stars real,
    cool          int4,
    fans          int4,
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
    latitude     double precision,
    longitude    double precision,
    name         text,
    postal_code  varchar(32),
    review_count int4,
    stars        real,
    state        varchar(3)
);


CREATE TABLE IF NOT EXISTS staging_checkins
(
    business_id char(22),
    date        timestamp
);


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


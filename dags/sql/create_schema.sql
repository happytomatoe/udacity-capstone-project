-- FACTS ---

CREATE TABLE IF NOT EXISTS "fact_review" (
  "review_id" char(22) PRIMARY KEY,
  "user_id" char(22),
  "business_id" char(22),
  "stars" int2,
  "date" date,
  "text" varchar(5000),
  "usefull" int4,
  "funny" int4,
  "cool" int4
);


-- TODO: do I need spark for this?
CREATE TABLE IF NOT EXISTS "fact_business_category" (
  "category" text,
  "business_id" char(22)
);



-- ***DIMENSIONS*** ---

CREATE TABLE IF NOT EXISTS "dim_user" (
  "user_id" char(22) PRIMARY KEY,
  "name" text,
  "yelping_since" timestamp,
--   TODO: do we need review_count?
  "usefull" int4,
  "funny" int4,
  "cool" int4,
  "fans" int4,
  "avg_stars" int4
);

-- TODO: add date dimension
-- FIXME: lat, lon converted to int
CREATE TABLE IF NOT EXISTS "dim_business" (
  "business_id" char(22) PRIMARY KEY,
  "name" text,
  "address" text,
  "city" text,
  "state" char(2),
--   TODO: change
  "postal_code" varchar(32),
  "latitude" double precision,
  "longitude" double precision,
  "stars" real,
  "review_count" int4,
  "is_open" boolean
);

CREATE TABLE IF NOT EXISTS "dim_tip" (
  "business_id" char(22),
  "user_id" char(22),
  "text" text,
  "compliment_count" int4,
  "create_date" date
);

ALTER TABLE "fact_review" ADD FOREIGN KEY ("user_id") REFERENCES "dim_user" ("user_id");

ALTER TABLE "fact_review" ADD FOREIGN KEY ("business_id") REFERENCES "dim_business" ("business_id");

ALTER TABLE "fact_business_category" ADD FOREIGN KEY ("business_id") REFERENCES "dim_business" ("business_id");

ALTER TABLE "dim_tip" ADD FOREIGN KEY ("business_id") REFERENCES "dim_business" ("business_id");

ALTER TABLE "dim_tip" ADD FOREIGN KEY ("user_id") REFERENCES "dim_user" ("user_id");


-- STAGING ---

CREATE TABLE IF NOT EXISTS staging_users
(
    user_id char(22),
    average_stars real,
    compliment_cool int4,
    compliment_cute int4,
    compliment_funny int4,
    compliment_hot int4,
    compliment_list int4,
    compliment_more int4,
    compliment_note int4,
    compliment_photos int4,
    compliment_plain int4,
    compliment_profile int4,
    compliment_writer int4,
    cool int4,
    elite text,
    fans int4,
    friends text,
    funny int4,
    name text,
    review_count int4,
    useful int4,
--     TODO: Do we need timestamp or should I use date or int YYYYMMDD?
    yelping_since timestamp
);

CREATE TABLE IF NOT EXISTS staging_reviews
(
    review_id char(22),
    user_id char(22),
    "business_id" char(22),
    cool int4,
    date timestamp,
    funny int4,
    stars real,
    text varchar(5000),
    useful int4
);

CREATE TABLE IF NOT EXISTS staging_businesses
(
    business_id char(22),
    address text,
    categories text,
    city text,
    is_open boolean,
--     TODO: check if need to change
    latitude double precision,
    longitude double precision,
    name text,
    postal_code varchar(32),
    review_count int4,
    stars real,
    state char(2)
);


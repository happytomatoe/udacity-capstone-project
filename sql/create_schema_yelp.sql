CREATE TABLE "review_fact" (
  "review_id" char(22) PRIMARY KEY,
  "user_id" char(22),
  "business_id" char(22),
  "stars" real,
  "date" date,
  "text" varchar(5000),
  "usefull" int2,
  "funny" int2,
  "cool" int2
);

CREATE TABLE "business_dim" (
  "business_id" char(22) PRIMARY KEY,
  "name" varchar,
  "adress" varchar,
  "city" varchar,
  "state" char(2),
  "postal_code" varchar,
  "latitude" numeric,
  "longitude" numeric,
  "stars" real,
  "review_count" int2,
  "is_open" boolean
);

CREATE TABLE "business_dim_category" (
  "category" varchar,
  "business_id" char(22)
);

CREATE TABLE "user_dim" (
  "user_id" char(22) PRIMARY KEY,
  "name" varchar,
  "yelping_since" date,
  "usefull" int2,
  "funny" int2,
  "cool" int2,
  "fans" int,
  "avg_stars" int
);

CREATE TABLE "tip_dim" (
  "business_id" char(22),
  "user_id" char(22),
  "text" varchar,
  "compliment_count" int,
  "create_date" date
);

ALTER TABLE "review_fact" ADD FOREIGN KEY ("user_id") REFERENCES "user_dim" ("user_id");

ALTER TABLE "review_fact" ADD FOREIGN KEY ("business_id") REFERENCES "business_dim" ("business_id");

ALTER TABLE "business_dim_category" ADD FOREIGN KEY ("business_id") REFERENCES "business_dim" ("business_id");

ALTER TABLE "tip_dim" ADD FOREIGN KEY ("business_id") REFERENCES "business_dim" ("business_id");

ALTER TABLE "tip_dim" ADD FOREIGN KEY ("user_id") REFERENCES "user_dim" ("user_id");


-- staging

create table staging_users
(
    average_stars double precision,
    compliment_cool bigint,
    compliment_cute bigint,
    compliment_funny bigint,
    compliment_hot bigint,
    compliment_list bigint,
    compliment_more bigint,
    compliment_note bigint,
    compliment_photos bigint,
    compliment_plain bigint,
    compliment_profile bigint,
    compliment_writer bigint,
    cool bigint,
    elite text,
    fans bigint,
    friends text,
    funny bigint,
    name text,
    review_count bigint,
    useful bigint,
    user_id text,
    yelping_since text
);


create table staging_reviews
(
    business_id text,
    cool bigint,
    date text,
    funny bigint,
    review_id text,
    stars double precision,
    text text,
    useful bigint,
    user_id text
);


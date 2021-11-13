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

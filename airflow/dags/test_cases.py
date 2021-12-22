"""
    Test cases for data quality check
"""

from helpers import TestCase

test_cases = [
    # Check if tables are empty
    TestCase("SELECT  COUNT(*)>0 FROM fact_review", True),
    TestCase("SELECT  COUNT(*)>0 FROM fact_checkin", True),
    TestCase("SELECT  COUNT(*)>0 FROM fact_tip", True),
    TestCase("SELECT  COUNT(*)>0 FROM fact_business_category", True),
    TestCase("SELECT  COUNT(*)>0 FROM dim_business", True),
    TestCase("SELECT  COUNT(*)>0 FROM dim_user", True),
    # Individual checks
    TestCase("SELECT  COUNT(*)>0 FROM dim_user WHERE name is null or trim(name) = ''", False),
    TestCase("SELECT  COUNT(*)>0 FROM dim_user WHERE user_id is null", False),
    TestCase("SELECT  COUNT(*)>0 FROM dim_business WHERE business_id is null", False),
    TestCase("SELECT  COUNT(*)>0 FROM dim_business WHERE name is null or trim(name) = ''", False),

    TestCase("SELECT  COUNT(*)>0 FROM fact_review WHERE review_id is null or trim(review_id) = ''", False),
    TestCase("SELECT  COUNT(*)>0 FROM fact_review WHERE user_id is null or trim(user_id) = ''", False),
    TestCase("SELECT  COUNT(*)>0 FROM fact_review WHERE business_id is null or trim(business_id) = ''", False),
    TestCase("SELECT  COUNT(*)>0 FROM fact_review WHERE text is null or trim(text) = ''", False),
    TestCase("SELECT  COUNT(*)>0 FROM fact_review WHERE stars<1 or stars>5", False),

    TestCase("SELECT  COUNT(*)>0 FROM fact_business_category WHERE category is null or trim(category) = ''", False),
    TestCase("SELECT  COUNT(*)>0 FROM fact_business_category WHERE business_id is null or trim(business_id) = ''",
             False),

    TestCase("SELECT  COUNT(*)>0 FROM fact_tip WHERE user_id is null or trim(user_id) = ''", False),
    TestCase("SELECT  COUNT(*)>0 FROM fact_tip WHERE business_id is null or trim(business_id) = ''", False),
    TestCase("SELECT  COUNT(*)>0 FROM fact_tip WHERE text is null or trim(text) = ''", False),

    TestCase("SELECT  COUNT(*)>0 FROM fact_checkin WHERE business_id is null or trim(business_id) = ''", False),
    TestCase("SELECT  COUNT(*)>0 FROM fact_checkin WHERE timestamp is null or trim(timestamp) = ''", False),

    TestCase("SELECT  COUNT(*)>0 FROM fact_friend WHERE user_id is null or trim(user_id) = ''", False),
    TestCase("SELECT  COUNT(*)>0 FROM fact_friend WHERE friend_id is null or trim(friend_id) = ''", False),
]
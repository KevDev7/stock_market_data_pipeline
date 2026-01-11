from utilities.snowflake_helper import query_snowflake

df = query_snowflake("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
print(df)
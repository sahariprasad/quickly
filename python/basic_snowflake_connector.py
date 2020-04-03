import snowflake.connector
from snowflake.connector import DictCursor
snowflake.connector.paramstyle = 'qmark'

snowflakeConn = snowflake.connector.connect(
        user='user',
        password='password',
        account='account',
        warehouse='warehouse',
        database='database',
        schema='schema'
    )

sfcursor = snowflakeConn.cursor(DictCursor)

sfcursor.execute('select 1 as id')
for row in sfcursor:
    print(row)
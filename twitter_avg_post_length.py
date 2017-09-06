# HBASE Database Design Here : https://github.com/jerinisready/Kafka-Twittter-Streaming
from __future__ import print_function
import json
import happybase
# from happybase import connection as Connection
## START bin/hbase rest start -p <port>


## VARS
LISTENING_TOPIC = 'myWorld'
LOG = True
## HBASE TABLE SPECIFIC ##
MASTER = '192.168.1.21'
PORT = 8080
LOCALHOST = 'localhost'
HBASE_TABLE_NAME = 'twitter'
HBASE_TABLE_COLUMN_FAMILY_NAME = "json"
ROW_NAME = "tweets"

HBASE_TABLE_VALUE_COLUMN_NAME = "data"
HBASE_TABLE_TIME_COLUMN_NAME = "timestamp"
HBASE_TABLE_KEY_COLUMN_NAME = "key"
HBASE_TABLE_TOPIC_COLUMN_NAME = "topic"

## HBASE CONNECTON
if LOG : print (" [+] Establishing Connection with HBASE via HappyBase")
connection = happybase.Connection(MASTER)

# if LOG : print (" [+] AVAILABLE  TABLE(S) :: ", connection.tables() )

if LOG : print (" [+] LOADING TABLE  %s" %  HBASE_TABLE_NAME)
hbase_table = connection.table(HBASE_TABLE_NAME)

# if not hbase_table.exists():
#     if LOG : print (" [+] CREATING NEW TABLE <%s> WITH ROWS  (<%s>, )" % ( HBASE_TABLE_NAME, HBASE_TABLE_COLUMN_FAMILY_NAME ) )
#     hbase_table = hbase_table.create(HBASE_TABLE_COLUMN_FAMILY_NAME)

# if LOG : print (" [+] AVAILABLE COLUMN(S)", hbase_table.columns())
# By default, prior further execution of the fetch, insert, update, remove (table row operations) methods,
# it's being checked whether the table exists or not
# hbase_table.check_if_exists_on_row_fetch = False


######################################################
def to_len(content):
    try:
        a = content[1]
        b =  json.loads(a['json:data'])['text']
        return len(b.split())
    except:
        print("FAIL")
        return 0

######################################################



if LOG : print ( " [+] FETCHING DATA FROM HBASE" )
hbase_data  = hbase_table.scan(row_prefix=b'tweets', columns=[b'json:data'])

# create Spark context with Spark configuration
if LOG : print ( " [+] Creating SPARK Session" )
sc = SparkContext(appName="HBaseInputFormat" )

if LOG : print ( " [+] Parallellizing " )
hbase_rdd = sc.parallelize( hbase_data )
if LOG : print ( " [ ] RDD OBJ :::::\n\n", hbase_rdd )
if LOG : print ( " [ ] RDD GENERATION SUCCESSFUL :::::\n\n" )
if LOG : print ( " [+] Reducing..." )
hbase_rdd = hbase_rdd.map( to_len )
hbase_rdd = hbase_rdd.reduce(lambda x,y :  (x + y) / 2 ) 

print (" [=] Average Length of Twitter Posts is : ", hbase_rdd)  # int


print(' [x] CLOSED ')

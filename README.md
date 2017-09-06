### HBase-Spark-Twitter-API-Streaming-Python
A sample Code to find Average length of tweets in Twitter.

#### Prerequests:

HBASE database : with table name `"twitter"` and a column family `"json"` feeded with `twitter api stream`.
Each row name starts with `tweet`

Used Happybase to connect

HBASE Database structure is mentioned in 
> https://github.com/jerinisready/Kafka-Twittter-Streaming


#### Algorithm
1. Takes length of each Tweet.
2. find average by Reducing It.


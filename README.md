# dynamoplus-java
A Java library for leveraging GSI overloading on AWS Dynamo DB

## Info

This library is a Java version of https://github.com/antessio/dynamoplus. 

The idea is to separate the main table (called `domain`) from the metadata table (called `system`).
Both tables have:
- a primary key : `pk` as partition key and `sk` as sort key
- a global secondary index : `sk` as partition key and `data` as sort key

DynamoPlus uses a separate Dynamo DB table to stores useful metadata to build Dynamo DB record using the GSI overloading. 

Basically once a document is stored in the `domain` table, the record is processed (using Dynamo DB streams or other asynchronous methods) and for each `index` found for its `collection` create a new record to store duplicated data needed for indexing through Global Secondary Index. 


package antessio.dynamoplus.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.Map;

public class DynamoDb {
    private final AmazonDynamoDB client;
    private final String tableName;


    public DynamoDb(AmazonDynamoDB client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    public Map<String, AttributeValue> getItem(Map<String, AttributeValue> key) {
        GetItemRequest request = new GetItemRequest()
                .withTableName(tableName)
                .withAttributesToGet("pk", "sk", "data", "document")
                .withKey(key);
        return client.getItem(request).getItem();
    }

    public QueryResult query(Map<String, Condition> keyConditions,
                             Map<String, AttributeValue> exclusiveStartKey,
                             Integer limit,
                             String indexName) {
        QueryRequest queryRequest = new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(keyConditions)
                .withExclusiveStartKey(exclusiveStartKey)
                .withIndexName(indexName)
                .withLimit(limit);
        return client.query(queryRequest);
    }

    public Map<String, AttributeValue> insert(Map<String, AttributeValue> item) {
        PutItemRequest putItemRequest = new PutItemRequest()
                .withItem(item)
                .withReturnValues(ReturnValue.ALL_OLD)
                .withTableName(tableName);
        PutItemResult response = client.putItem(putItemRequest);
        //logging
        return item;
    }

    public Map<String, AttributeValue> update(Map<String, AttributeValueUpdate> item) {
        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(tableName)
                .withReturnValues(ReturnValue.ALL_NEW)
                .withAttributeUpdates(item);
        return client.updateItem(updateItemRequest).getAttributes();
    }

    public Map<String, AttributeValue> delete(Map<String, AttributeValue> key) {
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
                .withTableName(tableName)
                .withKey(key);
        return client.deleteItem(deleteItemRequest).getAttributes();
    }

}

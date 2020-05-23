package antessio.dynamoplus.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.Map;

public class DynamoDb {
    private final AmazonDynamoDB client;


    public DynamoDb(AmazonDynamoDB client) {
        this.client = client;
    }

    public Map<String, AttributeValue> getItem(Map<String, AttributeValue> key, String tableName) {
        GetItemRequest request = new GetItemRequest()
                .withTableName(tableName)
                .withAttributesToGet("pk", "sk", "data", "document")
                .withKey(key);
        return client.getItem(request).getItem();
    }

    public QueryResult query(Map<String, Condition> keyConditions,
                             Map<String, AttributeValue> exclusiveStartKey,
                             Integer limit,
                             String indexName, String tableName) {
        QueryRequest queryRequest = new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(keyConditions)
                .withExclusiveStartKey(exclusiveStartKey)
                .withIndexName(indexName)
                .withLimit(limit);
        return client.query(queryRequest);
    }

    public Map<String, AttributeValue> insert(Map<String, AttributeValue> item, String tableName) {
        PutItemRequest putItemRequest = new PutItemRequest()
                .withItem(item)
                .withReturnValues(ReturnValue.ALL_OLD)
                .withTableName(tableName);
        PutItemResult response = client.putItem(putItemRequest);
        //logging
        return item;
    }

    public Map<String, AttributeValue> update(Map<String, AttributeValue> key, Map<String, AttributeValueUpdate> item, String tableName) {
        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(tableName)
                .withReturnValues(ReturnValue.ALL_NEW)
                .withKey(key)
                .withAttributeUpdates(item);
        return client.updateItem(updateItemRequest).getAttributes();
    }

    public void delete(Map<String, AttributeValue> key, String tableName) {
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
                .withTableName(tableName)
                .withReturnValues(ReturnValue.ALL_OLD)
                .withKey(key);
        DeleteItemResult result = client.deleteItem(deleteItemRequest);
        if (result.getSdkHttpMetadata().getHttpStatusCode() != 200) {
            throw new RuntimeException(String.format("record not deleted (%d)", result.getSdkHttpMetadata().getHttpStatusCode()));
        }
        ;
    }

}

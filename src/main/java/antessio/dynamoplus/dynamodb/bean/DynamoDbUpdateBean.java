package antessio.dynamoplus.dynamodb.bean;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;

import java.util.Map;

public class DynamoDbUpdateBean {
    Map<String, AttributeValue> key;
    Map<String, AttributeValueUpdate> value;

    public DynamoDbUpdateBean(Map<String, AttributeValue> key, Map<String, AttributeValueUpdate> value) {
        this.key = key;
        this.value = value;
    }

    public Map<String, AttributeValue> getKey() {
        return key;
    }

    public Map<String, AttributeValueUpdate> getValue() {
        return value;
    }
}

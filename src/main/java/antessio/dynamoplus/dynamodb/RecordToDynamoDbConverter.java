package antessio.dynamoplus.dynamodb;

import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.RecordBuilder;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;


import java.util.HashMap;
import java.util.Map;

import static antessio.dynamoplus.utils.MapUtil.entry;
import static java.util.stream.Collectors.toMap;

public final class RecordToDynamoDbConverter {

    private RecordToDynamoDbConverter() {

    }

    public static Map<String, AttributeValue> toDynamo(Record record) {
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        attributeValueMap.put("pk", new AttributeValue().withS(record.getPk()));
        attributeValueMap.put("sk", new AttributeValue().withS(record.getSk()));
        attributeValueMap.put("data", new AttributeValue().withS(record.getData()));
        attributeValueMap.put("document", ItemUtils.toAttributeValue(record.getDocument()));
        return attributeValueMap;
    }

    public static Record fromDynamo(Map<String, AttributeValue> item) {
        String pk = item.get("pk").getS();
        String sk = item.get("sk").getS();
        String data = item.get("data").getS();
        Map<String, Object> document = ItemUtils.toItem(item.get("document").getM()).asMap();
        return RecordBuilder.aRecord()
                .withDocument(document)
                .withData(data)
                .withSk(sk)
                .withPk(pk)
                .build();
    }

    public static Map<String, AttributeValueUpdate> toDynamoUpdate(Record record) {
        return toDynamo(record)
                .entrySet()
                .stream()
                .map(e -> entry(e.getKey(),
                        new AttributeValueUpdate()
                                .withAction(AttributeAction.PUT)
                                .withValue(e.getValue()),
                        AttributeValueUpdate.class))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}

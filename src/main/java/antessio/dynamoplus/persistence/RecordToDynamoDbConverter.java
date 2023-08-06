package antessio.dynamoplus.persistence;

import antessio.dynamoplus.persistence.bean.DynamoDbUpdateBean;
import antessio.dynamoplus.persistence.bean.Record;
import antessio.dynamoplus.persistence.bean.RecordBuilder;
import antessio.dynamoplus.persistence.bean.RecordKey;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.utils.ConversionUtils;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;


import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static antessio.dynamoplus.utils.MapUtil.entry;
import static java.util.stream.Collectors.toMap;

public final class RecordToDynamoDbConverter {

    private RecordToDynamoDbConverter() {

    }

    public static Map<String, AttributeValue> toDynamo(Record record) {
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        Optional.ofNullable(record.getRecordKey().getPk())
                .ifPresent(pk -> attributeValueMap.put("pk", new AttributeValue().withS(pk)));
        Optional.ofNullable(record.getRecordKey().getSk())
                .ifPresent(sk -> attributeValueMap.put("sk", new AttributeValue().withS(sk)));
        Optional.ofNullable(record.getData())
                .ifPresent(data -> attributeValueMap.put("data", new AttributeValue().withS(data)));
        Optional.ofNullable(record.getDocument())
                .map(ConversionUtils.getInstance()::convertToJson)
                .map(ItemUtils::toAttributeValue)
                .ifPresent(document -> attributeValueMap.put("document", document));

        return attributeValueMap;
    }

    public static Record fromDynamo(Map<String, AttributeValue> item) {
        String pk = item.get("pk").getS();
        String sk = item.get("sk").getS();
        String data = item.get("data").getS();
        Document document = Optional.ofNullable(item.get("document"))
                .map(AttributeValue::getS)
                .map(ConversionUtils.getInstance()::fromJson)
                .map(dict -> new Document(dict, pk.split("#")[1]))
                .orElseThrow(() -> new RuntimeException("document value not found from dynamo"));
        return RecordBuilder.aRecord()
                .withDocument(document)
                .withData(data)
                .withRecordKey(new RecordKey(pk,sk))
                .build();
    }

    public static DynamoDbUpdateBean toDynamoUpdate(Record record) {

        Map<String, AttributeValue> attributeValueMap = toDynamo(record);
        Map<String, AttributeValue> keyMap = attributeValueMap
                .entrySet()
                .stream()
                .filter(e -> e.getKey().equals("pk") || e.getKey().equals("sk"))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        ;
        Map<String, AttributeValueUpdate> updateMap = attributeValueMap
                .entrySet()
                .stream()
                .filter(e -> !e.getKey().equals("pk") && !e.getKey().equals("sk"))
                .map(e -> entry(e.getKey(),
                        new AttributeValueUpdate()
                                .withAction(AttributeAction.PUT)
                                .withValue(e.getValue()),
                        AttributeValueUpdate.class))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new DynamoDbUpdateBean(keyMap, updateMap);
    }
}

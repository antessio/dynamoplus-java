package antessio.dynamoplus.dynamodb;

import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.RecordBuilder;

import antessio.dynamoplus.utils.DynamoPlusUtils;
import antessio.dynamoplus.system.bean.collection.Collection;
import antessio.dynamoplus.system.bean.index.Index;


import java.util.Map;
import java.util.Optional;

public final class RecordFactory {

    private RecordFactory() {

    }

    private static RecordFactory instance;

    public static RecordFactory getInstance() {
        if (instance == null) {
            return new RecordFactory();
        }
        return instance;
    }

    public Record masterRecordFromDocument(Map<String, Object> document, Collection collection) {
        String idKey = getIdKeyFromCollection(document, collection)
                .orElseThrow(() -> new RuntimeException("the document have no value for key " + collection.getIdKey()));
        String sk = collection.getName();
        String data = getOrdering_key(document, "order_unique")
                .orElse(idKey);
        return RecordBuilder.aRecord()
                .withPk(String.format("%s#%s", sk, idKey))
                .withSk(sk)
                .withData(data)
                .withDocument(document)
                .build();
    }

    private Optional<String> getIdKeyFromCollection(Map<String, Object> document, Collection collection) {
        return DynamoPlusUtils.getValueRecursively(collection.getIdKey(), document)
                .filter(o -> o instanceof String)
                .map(String.class::cast);
    }


    public Record indexingRecordFromDocument(Map<String, Object> document, Index index) {
        Collection collection = index.getCollection();
        Optional<String> maybeIdKey = getIdKeyFromCollection(document, collection);

        Map<String, String> valuesInIndex = DynamoPlusUtils.getIndexingMap(document, index.getConditions());
        String keysString = String.join("#", index.getConditions());
        String valuesString = String.join("#", valuesInIndex.values());
        String data = getOrdering_key(document, index.getOrderingKey())
                .map(orderingKey -> String.format("%s#%s", valuesString, orderingKey))
                .orElse(valuesString);

        return RecordBuilder.aRecord()
                .withPk(maybeIdKey.map(idKey -> String.format("%s#%s", collection.getName(), idKey)).orElse(null))
                .withSk(String.format("%s#%s", collection.getName(), keysString))
                .withData(data)
                .withDocument(document)
                .build();
    }

    private Optional<String> getOrdering_key(Map<String, Object> document, String orderingKey) {
        return Optional.ofNullable(orderingKey)
                .flatMap(k -> DynamoPlusUtils.getValueRecursively(k, document)
                        .filter(o -> o instanceof Long)
                        .map(Long.class::cast)
                        .map(Object::toString)
                );
    }
}

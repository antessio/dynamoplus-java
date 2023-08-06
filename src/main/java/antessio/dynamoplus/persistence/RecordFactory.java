package antessio.dynamoplus.persistence;

import antessio.dynamoplus.persistence.bean.Record;
import antessio.dynamoplus.persistence.bean.RecordBuilder;

import antessio.dynamoplus.persistence.bean.RecordKey;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.utils.DynamoPlusUtils;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.index.Index;


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

    public Record masterRecordFromDocument(Document document, Collection collection) {
        String idKey = getIdKeyFromCollection(document, collection)
                .orElseThrow(() -> new RuntimeException("the document have no value for key " + collection.getIdKey()));
        String sk = collection.getName();
        String data = getOrdering_key(document, "order_unique")
                .orElse(idKey);
        return RecordBuilder.aRecord()
                            .withRecordKey(new RecordKey(String.format("%s#%s", sk, idKey), sk))
                            .withData(data)
                            .withDocument(document)
                            .build();
    }

    private Optional<String> getIdKeyFromCollection(Document document, Collection collection) {
        return DynamoPlusUtils.getValueRecursively(collection.getIdKey(), document)
                              .filter(o -> o instanceof String)
                              .map(String.class::cast);
    }


    public Record indexingRecordFromDocument(Document document, Index index) {
        Collection collection = index.getCollection();
        Optional<String> maybeIdKey = getIdKeyFromCollection(document, collection);

        Map<String, String> valuesInIndex = DynamoPlusUtils.getIndexingMap(document, index.getConditions());
        String keysString = String.join("#", index.getConditions());
        String valuesString = String.join("#", valuesInIndex.values());
        String data = getOrdering_key(document, index.getOrderingKey())
                .map(orderingKey -> String.format("%s#%s", valuesString, orderingKey))
                .orElse(valuesString);

        return RecordBuilder.aRecord()
                            .withRecordKey(new RecordKey(
                                    maybeIdKey.map(idKey -> String.format("%s#%s", collection.getName(), idKey)).orElse(null),
                                    String.format("%s#%s", collection.getName(), keysString)))
                            .withData(data)
                            .withDocument(document)
                            .build();
    }

    private Optional<String> getOrdering_key(Document document, String orderingKey) {
        return Optional.ofNullable(orderingKey)
                       .flatMap(k -> DynamoPlusUtils.getValueRecursively(k, document)
                                                    .filter(o -> o instanceof Long)
                                                    .map(Long.class::cast)
                                                    .map(Object::toString)
                       );
    }

}

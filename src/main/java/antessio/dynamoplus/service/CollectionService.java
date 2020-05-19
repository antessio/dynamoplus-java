package antessio.dynamoplus.service;

import antessio.dynamoplus.dynamodb.RecordFactory;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.system.bean.collection.*;
import antessio.dynamoplus.utils.ConversionUtils;

import java.util.Map;

import static antessio.dynamoplus.utils.MapUtil.entry;
import static antessio.dynamoplus.utils.MapUtil.ofEntries;

public class CollectionService {

    public static final String NAME = "name";
    public static final String ID_KEY = "id_key";
    public static final String ATTRIBUTES = "attributes";
    public static final String ATTRIBUTE_NAME = "name";
    public static final String ATTRIBUTE_TYPE = "type";
    public static final String ATTRIBUTE_SUB_ATTRIBUTES = "attributes";
    public static final String ATTRIBUTE_CONSTRAINTS = "constraints";

    private static final Collection COLLECTION_METADATA = new CollectionBuilder()
            .idKey("name")
            .name("collection")
            .autoGenerateId(false)
            .createCollection();

    private final DynamoDbTableRepository tableRepository;

    public CollectionService(DynamoDbTableRepository tableRepository) {
        this.tableRepository = tableRepository;
    }


    public static Map<String, Object> fromCollectionToMapMin(Collection collection) {
        return ofEntries(
                entry(NAME, collection.getName()),
                entry(ID_KEY, collection.getIdKey())
        );
    }

    public static Map<String, Object> fromCollectionToMap(Collection collection) {
        return ConversionUtils.getInstance().convertObject(collection);
    }


    public static Collection fromMapToCollection(Map<String, Object> document) {
        return ConversionUtils.getInstance().convertMap(document, Collection.class);
    }

    public Collection getCollectionByName(String name) {
        Collection collection = new CollectionBuilder()
                .name(name)
                .createCollection();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromCollectionToMap(collection), COLLECTION_METADATA);
        Record foundRecord = tableRepository.get(record.getPk(), record.getSk());
        return fromMapToCollection(foundRecord.getDocument());
    }

    public Collection createCollection(Collection collection) {
        Map<String, Object> collectionDocument = fromCollectionToMap(collection);
        Record record = RecordFactory.getInstance().masterRecordFromDocument(collectionDocument, COLLECTION_METADATA);
        Record recordCreated = tableRepository.create(record);
        return fromMapToCollection(recordCreated.getDocument());
    }

    public Collection updateCollection(Collection collection) {
        Map<String, Object> collectionDocument = fromCollectionToMap(collection);
        Record record = RecordFactory.getInstance().masterRecordFromDocument(collectionDocument, COLLECTION_METADATA);
        Record recordCreated = tableRepository.update(record);
        return fromMapToCollection(recordCreated.getDocument());
    }


    public void delete(String name) {
        Collection collection = new CollectionBuilder()
                .name(name)
                .createCollection();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromCollectionToMap(collection), COLLECTION_METADATA);
        Record foundRecord = tableRepository.delete(record.getPk(), record.getSk());
        Collection collectionRemoved = fromMapToCollection(foundRecord.getDocument());
    }


}

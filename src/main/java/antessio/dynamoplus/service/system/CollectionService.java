package antessio.dynamoplus.service.system;

import antessio.dynamoplus.dynamodb.RecordFactory;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.utils.ConversionUtils;

import java.util.Map;
import java.util.Optional;

import static antessio.dynamoplus.utils.MapUtil.entry;

public class CollectionService {


    private static final Collection COLLECTION_METADATA = new CollectionBuilder()
            .idKey("name")
            .name("collection")
            .autoGenerateId(false)
            .createCollection();

    private final DynamoDbTableRepository tableRepository;

    public CollectionService(DynamoDbTableRepository tableRepository) {
        this.tableRepository = tableRepository;
    }


    public static Map<String, Object> fromCollectionToMap(Collection collection) {
        return ConversionUtils.getInstance().convertObject(collection);
    }


    public static Collection fromMapToCollection(Map<String, Object> document) {
        return ConversionUtils.getInstance().convertMap(document, Collection.class);
    }

    public Optional<Collection> getCollectionByName(String name) {
        Collection collection = new CollectionBuilder()
                .name(name)
                .createCollection();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromCollectionToMap(collection), COLLECTION_METADATA);
        return tableRepository.get(record.getPk(), record.getSk())
                .map(Record::getDocument)
                .map(CollectionService::fromMapToCollection);
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
        tableRepository.delete(record.getPk(), record.getSk());
    }


}

package antessio.dynamoplus.service.system;

import antessio.dynamoplus.persistence.RecordFactory;
import antessio.dynamoplus.persistence.bean.Record;
import antessio.dynamoplus.persistence.bean.RecordKey;
import antessio.dynamoplus.persistence.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.system.bean.collection.*;
import antessio.dynamoplus.utils.ConversionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static antessio.dynamoplus.utils.MapUtil.entry;

public class CollectionService {


    public static final Collection COLLECTION_METADATA = CollectionBuilder.aCollection()
            .withIdKey("name")
            .withName("collection")
            .withAutoGenerateId(false)
            .withAttributes(
                    Arrays.asList(
                            new AttributeBuilder()
                                    .attributeName("id_key")
                                    .attributeType(CollectionAttributeType.STRING)
                                    .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                                    .build(),
                            new AttributeBuilder()
                                    .attributeName("auto_generate_id")
                                    .attributeType(CollectionAttributeType.BOOLEAN)
                                    .build(),
                            new AttributeBuilder()
                                    .attributeName("attributes")
                                    .attributeType(CollectionAttributeType.ARRAY)
                                    .attributes(Arrays.asList(
                                            new AttributeBuilder()
                                                    .attributeName("name")
                                                    .attributeType(CollectionAttributeType.STRING)
                                                    .build(),
                                            new AttributeBuilder()
                                                    .attributeName("type")
                                                    .attributeType(CollectionAttributeType.STRING)
                                                    .build(),
                                            new AttributeBuilder()
                                                    .attributeName("constraints")
                                                    .attributeType(CollectionAttributeType.ARRAY)
                                                    .build(),
                                            new AttributeBuilder()
                                                    .attributeName("attributes")
                                                    .attributeType(CollectionAttributeType.ARRAY)
                                                    .build()
                                    ))
                                    .build()
                    )
            )
            .build();

    private final DynamoDbTableRepository tableRepository;

    public CollectionService(DynamoDbTableRepository tableRepository) {
        this.tableRepository = tableRepository;
    }


    public static Document fromCollectionToMap(Collection collection) {
        return ConversionUtils.getInstance().convertObject(collection);
    }


    public static Collection fromMapToCollection(Document document) {
        return ConversionUtils.getInstance().convertDocument(document, Collection.class);
    }

    public Optional<Collection> getCollectionByName(String name) {
        Collection collection = CollectionBuilder.aCollection()
                .withName(name)
                .build();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromCollectionToMap(collection), COLLECTION_METADATA);
        return tableRepository.get(record.getRecordKey())
                .map(Record::getDocument)
                .map(CollectionService::fromMapToCollection);
    }

    public Collection createCollection(Collection collection) {
        Document collectionDocument = fromCollectionToMap(collection);
        Record record = RecordFactory.getInstance().masterRecordFromDocument(collectionDocument, COLLECTION_METADATA);
        Record recordCreated = tableRepository.create(record);
        return fromMapToCollection(recordCreated.getDocument());
    }

    public Collection updateCollection(Collection collection) {
        Document collectionDocument = fromCollectionToMap(collection);
        Record record = RecordFactory.getInstance().masterRecordFromDocument(collectionDocument, COLLECTION_METADATA);
        Record recordCreated = tableRepository.update(record);
        return fromMapToCollection(recordCreated.getDocument());
    }


    public void delete(String name) {
        Collection collection = CollectionBuilder.aCollection()
                .withName(name)
                .build();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromCollectionToMap(collection), COLLECTION_METADATA);
        tableRepository.delete(record.getRecordKey());
    }


}

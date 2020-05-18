package antessio.dynamoplus.service;

import antessio.dynamoplus.dynamodb.RecordFactory;
import antessio.dynamoplus.dynamodb.bean.QueryBuilder;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.query.Eq;
import antessio.dynamoplus.dynamodb.bean.query.PredicateBuilder;
import antessio.dynamoplus.dynamodb.bean.query.QueryResultsWithCursor;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.system.bean.collection.Collection;
import antessio.dynamoplus.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.system.bean.index.Index;
import antessio.dynamoplus.system.bean.index.IndexBuilder;
import antessio.dynamoplus.utils.ConversionUtils;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static antessio.dynamoplus.utils.DynamoPlusUtils.safeGet;
import static antessio.dynamoplus.utils.MapUtil.entry;
import static antessio.dynamoplus.utils.MapUtil.ofEntries;

public class IndexService {

    public static final Collection INDEX_COLLECTION = new CollectionBuilder()
            .name("index")
            .idKey("uid")
            .autoGenerateId(true)
            .createCollection();
    public static final Index INDEX_FOR_INDEX_COLLECTION = new IndexBuilder()
            .collection(INDEX_COLLECTION)
            .conditions(new ArrayList<>(Arrays.asList("collection.name", "name")))
            .createIndex();
    public static final String ORDERING_KEY = "ordering_key";
    public static final String ID = "uid";
    public static final String NAME = "name";
    public static final String COLLECTION = "collection";
    public static final String CONDITIONS = "conditions";
    private final DynamoDbTableRepository tableRepository;


    public IndexService(
            DynamoDbTableRepository tableRepository) {
        this.tableRepository = tableRepository;
    }

    public Index createIndex(Index index) {
        Map<String, Object> indexDocument = fromIndexToMap(index);
        Record record = RecordFactory.getInstance().masterRecordFromDocument(indexDocument, INDEX_COLLECTION);
        Record recordCreated = tableRepository.create(record);
        return fromMapToIndex(recordCreated.getDocument());
    }

    public void createGsiRows(Index index) {
        Map<String, Object> indexAsDocument = fromIndexToMap(index);
        Record record = RecordFactory.getInstance()
                .indexingRecordFromDocument(indexAsDocument, INDEX_FOR_INDEX_COLLECTION);
        tableRepository.create(record);
    }

    public static Index fromMapToIndex(Map<String, Object> document) {

        return ConversionUtils.getInstance().convertMap(document, Index.class);
//        return new IndexBuilder()
//                .uid(UUID.fromString(safeGet(document, String.class, ID)))
//                .orderingKey(safeGet(document, String.class, ORDERING_KEY))
//                .name(safeGet(document, String.class, NAME))
//                .conditions(safeGet(document, List.class, CONDITIONS))
//                .collection(CollectionService.fromMapToCollection(safeGet(document, Map.class, COLLECTION)))
//                .createIndex();
    }


    public static Map<String, Object> fromIndexToMap(Index index) {
        return ConversionUtils.getInstance().convertObject(index);
//        Stream<Supplier<AbstractMap.SimpleEntry<String, Object>>> chain = Stream.of(
//                () -> Optional.ofNullable(index.getUid()).map(id -> entry(ID, id)).orElse(null),
//                () -> Optional.ofNullable(index.getName()).map(name -> entry(NAME, name)).orElse(null),
//                () -> Optional.ofNullable(index.getCollection()).map(CollectionService::fromCollectionToMapMin).map(collection -> entry(COLLECTION, collection)).orElse(null),
//                () -> Optional.ofNullable(index.getOrderingKey()).map(orderingKey -> entry(ORDERING_KEY, orderingKey)).orElse(null),
//                () -> Optional.ofNullable(index.getConditions()).map(conditions -> entry(CONDITIONS, conditions)).orElse(null)
//        );
//        return ofEntries(
//                chain
//                        .map(Supplier::get)
//                        .filter(Objects::nonNull)
//                        .toArray(AbstractMap.SimpleEntry[]::new)
//        );
//        return ofEntries(
//                entry(ID, index.getUid().toString()),
//                entry(NAME, index.getName()),
//                entry(COLLECTION, Optional.ofNullable(index.getCollection()).map(CollectionService::fromCollectionToMapMin).orElse(null)),
//                entry(ORDERING_KEY, index.getOrderingKey()),
//                entry(CONDITIONS, index.getConditions())
//        );
    }


    public Index getById(UUID id) {
        Index index = new IndexBuilder()
                .uid(id)
                .createIndex();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromIndexToMap(index), INDEX_COLLECTION);
        Record foundRecord = tableRepository.get(record.getPk(), record.getSk());
        return fromMapToIndex(foundRecord.getDocument());
    }

    public Optional<Index> getByCollectionName(String collectionName) {
        Index index = new IndexBuilder()
                .collection(collectionName)
                .createIndex();
        Record record = RecordFactory.getInstance().indexingRecordFromDocument(fromIndexToMap(index), INDEX_FOR_INDEX_COLLECTION);
        QueryResultsWithCursor results = tableRepository.query(QueryBuilder.aQuery()
                .withPartitionKey(record.getSk())
                .withPredicate(new Eq("collection.name", collectionName))
                .build());
        return results.getRecords().stream().findFirst()
                .map(Record::getDocument)
                .map(IndexService::fromMapToIndex);
    }

    public Optional<Index> getByCollectionNameAndName(String collectionName, String name) {
        Index index = new IndexBuilder()
                .collection(collectionName)
                .name(name)
                .createIndex();
        Record record = RecordFactory.getInstance().indexingRecordFromDocument(fromIndexToMap(index), INDEX_FOR_INDEX_COLLECTION);
        QueryResultsWithCursor results = tableRepository.query(QueryBuilder.aQuery()
                .withPartitionKey(record.getSk())
                .withPredicate(new PredicateBuilder().withAnd(
                        new LinkedList<>(
                                Arrays.asList(
                                        new Eq("collection.name", collectionName),
                                        new Eq("name", name))
                        )
                ))
                .build());
        return results.getRecords().stream().findFirst()
                .map(Record::getDocument)
                .map(IndexService::fromMapToIndex);
    }

    public void deleteIndexById(UUID id) {
        Index index = new IndexBuilder()
                .uid(id)
                .createIndex();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromIndexToMap(index), INDEX_COLLECTION);
        tableRepository.delete(record.getPk(), record.getSk());
    }


}

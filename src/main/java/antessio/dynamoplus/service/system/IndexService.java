package antessio.dynamoplus.service.system;

import antessio.dynamoplus.dynamodb.RecordFactory;
import antessio.dynamoplus.dynamodb.bean.QueryBuilder;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.query.Eq;
import antessio.dynamoplus.dynamodb.bean.query.PredicateBuilder;
import antessio.dynamoplus.dynamodb.bean.query.QueryResultsWithCursor;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.service.system.bean.index.Index;
import antessio.dynamoplus.service.system.bean.index.IndexBuilder;
import antessio.dynamoplus.utils.ConversionUtils;

import java.util.*;

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
    private final DynamoDbTableRepository tableRepository;


    public IndexService(DynamoDbTableRepository tableRepository) {
        this.tableRepository = tableRepository;
    }

    public static Index fromMapToIndex(Map<String, Object> document) {
        return ConversionUtils.getInstance().convertMap(document, Index.class);
    }


    public static Map<String, Object> fromIndexToMap(Index index) {
        return ConversionUtils.getInstance().convertObject(index);
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


    public Optional<Index> getById(UUID id) {
        Index index = new IndexBuilder()
                .uid(id)
                .createIndex();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromIndexToMap(index), INDEX_COLLECTION);
        return tableRepository.get(record.getPk(), record.getSk())
                .map(Record::getDocument)
                .map(IndexService::fromMapToIndex);
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

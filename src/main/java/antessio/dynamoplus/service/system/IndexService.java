package antessio.dynamoplus.service.system;

import antessio.dynamoplus.common.query.Predicate;
import antessio.dynamoplus.persistence.RecordFactory;
import antessio.dynamoplus.persistence.bean.QueryBuilder;
import antessio.dynamoplus.persistence.bean.Record;
import antessio.dynamoplus.common.query.Eq;
import antessio.dynamoplus.common.query.PredicateBuilder;
import antessio.dynamoplus.persistence.bean.query.QueryResultsWithCursor;
import antessio.dynamoplus.persistence.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.service.system.bean.index.Index;
import antessio.dynamoplus.service.system.bean.index.IndexBuilder;
import antessio.dynamoplus.utils.ConversionUtils;

import java.util.*;
import java.util.stream.Stream;

public class IndexService {

    public static final Collection INDEX_COLLECTION = CollectionBuilder.aCollection()
                                                                       .withName("index")
                                                                       .withIdKey("uid")
                                                                       .withAutoGenerateId(true)
                                                                       .build();
    public static final Index INDEX_FOR_INDEX_COLLECTION = IndexBuilder.anIndex()
                                                                       .withCollection(INDEX_COLLECTION)
                                                                       .withConditions(new ArrayList<>(Arrays.asList("collection.name", "name")))
                                                                       .build();
    private static final int BATCH_SIZE = 20;
    private final DynamoDbTableRepository tableRepository;


    public IndexService(DynamoDbTableRepository tableRepository) {
        this.tableRepository = tableRepository;
    }

    public static Index fromMapToIndex(Document document) {
        return ConversionUtils.getInstance().convertDocument(document, Index.class);
    }


    public static Document fromIndexToMap(Index index) {
        return ConversionUtils.getInstance().convertObject(index);
    }

    public Index createIndex(Index index) {
        Document indexDocument = fromIndexToMap(index);
        Record record = RecordFactory.getInstance().masterRecordFromDocument(indexDocument, INDEX_COLLECTION);
        Record recordCreated = tableRepository.create(record);
        return fromMapToIndex(recordCreated.getDocument());
    }


    public Optional<Index> getById(UUID id) {
        Index index = IndexBuilder.anIndex()
                                  .withUid(id)
                                  .build();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromIndexToMap(index), INDEX_COLLECTION);
        return tableRepository.get(record.getRecordKey())
                              .map(Record::getDocument)
                              .map(IndexService::fromMapToIndex);
    }

    public Stream<Index> getByCollectionName(String collectionName, UUID startingAfter) {
        Index index = IndexBuilder.anIndex()
                                  .withCollection(CollectionBuilder.aCollection().withName(collectionName).build())
                                  .build();
        Record record = RecordFactory.getInstance().indexingRecordFromDocument(fromIndexToMap(index), INDEX_FOR_INDEX_COLLECTION);
        Record startingFrom = Optional.ofNullable(startingAfter)
                                      .flatMap(this::getById)
                                      .map(i -> RecordFactory.getInstance()
                                                             .masterRecordFromDocument(
                                                                     fromIndexToMap(i),
                                                                     INDEX_COLLECTION))
                                      .orElse(null);
        return Stream.iterate(
                             getQuery(record, startingFrom, new Eq("collection.name", collectionName)),
                             r -> r.getRecords().size() >= BATCH_SIZE && r.getCursor() != null,
                             r -> getQuery(record, r.getCursor(), new Eq("collection.name", collectionName)))
                     .flatMap(q -> q.getRecords().stream())
                     .map(Record::getDocument)
                     .map(IndexService::fromMapToIndex);

    }

    private QueryResultsWithCursor getQuery(Record record, Record startingFrom, Predicate predicate) {
        return tableRepository.query(QueryBuilder.aQuery()
                                                 .withPartitionKey(record.getRecordKey().getSk())
                                                 .withLimit(BATCH_SIZE)
                                                 .withPredicate(predicate)
                                                 .withStartFrom(startingFrom)
                                                 .build());
    }

    public Optional<Index> getByCollectionNameAndName(String collectionName, String name) {
        Index index = IndexBuilder.anIndex()
                                  .withCollection(CollectionBuilder.aCollection().withName(collectionName).build())
                                  .withName(name)
                                  .build();
        Record record = RecordFactory.getInstance().indexingRecordFromDocument(fromIndexToMap(index), INDEX_FOR_INDEX_COLLECTION);
        QueryResultsWithCursor results = tableRepository.query(QueryBuilder.aQuery()
                                                                           .withPartitionKey(record.getRecordKey().getSk())
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
        Index index = IndexBuilder.anIndex()
                                  .withUid(id)
                                  .build();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromIndexToMap(index), INDEX_COLLECTION);
        tableRepository.delete(record.getRecordKey());
    }


    public Index updateIndex(Index indexToUpdate) {
        return null;
    }

}

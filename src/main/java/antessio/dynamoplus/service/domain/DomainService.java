package antessio.dynamoplus.service.domain;

import antessio.dynamoplus.common.query.Predicate;
import antessio.dynamoplus.persistence.RecordFactory;
import antessio.dynamoplus.persistence.bean.Query;
import antessio.dynamoplus.persistence.bean.QueryBuilder;
import antessio.dynamoplus.persistence.bean.Record;
import antessio.dynamoplus.persistence.bean.query.QueryResultsWithCursor;
import antessio.dynamoplus.persistence.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.bean.PaginatedResult;
import antessio.dynamoplus.service.system.bean.collection.Collection;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DomainService {

    public static final int BATCH_SIZE = 20;
    private final DynamoDbTableRepository repository;

    public DomainService(DynamoDbTableRepository repository) {
        this.repository = repository;
    }

    public static Document mergeDocuments(Document existingDocument, Document document, String idKey) {
        Map<String, Object> source = existingDocument.getDict();
        Map<String, Object> target = document.getDict().entrySet()
                                             .stream()
                                             .filter(e -> !e.getKey().equals(idKey))
                                             .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, Object> result = new LinkedHashMap<>();
        result.putAll(source);
        result.putAll(target);
        return new Document(result, existingDocument.getId());
    }


    public Document createDocument(Document document, Collection collection) {
        Record record = convertToRecord(collection, document);
        return repository.create(record).getDocument();
    }

    public Document updateDocument(Document document, Collection collection) {
        Record record = convertToRecord(collection, document);
        return repository.update(record).getDocument();
    }

    public Optional<Document> getDocument(String id, Collection collection) {
        Document document = new Document(collection.getIdKey(), id);
        Record record = convertToRecord(collection, document);
        return repository.get(record.getRecordKey())
                         .map(Record::getDocument);
    }

    public Stream<Document> query(Collection collection, String indexName, String startingFrom, Predicate predicate) {
        PaginatedResult<Document, String> firstPage = query(collection, predicate, indexName, Optional.ofNullable(startingFrom)
                                                                                                      .flatMap(lastKey -> getDocument(lastKey, collection))
                                                                                                      .map(lastDocument -> convertToRecord(
                                                                                                              collection,
                                                                                                              lastDocument))
                                                                                                      .orElse(null));
        return Stream.iterate(
                             firstPage,
                             r -> r.getNextId().isPresent(),
                             r -> query(collection, predicate, indexName,
                                        convertToRecord(collection, r.getData().get(r.getData().size() - 1))
                             ))
                     .map(PaginatedResult::getData)
                     .flatMap(java.util.Collection::stream);
    }

    private static Record convertToRecord(Collection collection, Document lastDocument) {
        return RecordFactory.getInstance()
                            .masterRecordFromDocument(
                                    lastDocument,
                                    collection);
    }

    public PaginatedResult<Document, String> query(
            antessio.dynamoplus.service.bean.Query query,
            Collection collection) {

        Query q = QueryBuilder.aQuery()
                              .withPredicate(query.getPredicate())
                              .withLimit(query.getLimit())
                              .withStartFrom(
                                      Optional.ofNullable(query.getLastKey())
                                              .flatMap(lastKey -> getDocument(lastKey, collection))
                                              .map(lastDocument -> convertToRecord(collection, lastDocument))
                                              .orElse(null)
                              )
                              .withPartitionKey(String.format("%s#%s", collection.getName(), query.getIndexName()))
                              .build();
        QueryResultsWithCursor results = repository.query(q);
        return new PaginatedResult<>(
                results.getRecords().stream().map(Record::getDocument).collect(Collectors.toList()),
                results.getCursor() != null,
                (String) results.getCursor().getDocument().get(collection.getIdKey()));
    }

    private PaginatedResult<Document, String> query(Collection collection, Predicate predicate, String indexName, Record startFrom) {

        QueryResultsWithCursor results = repository.query(QueryBuilder.aQuery()
                                                                      .withPredicate(predicate)
                                                                      .withLimit(BATCH_SIZE)
                                                                      .withStartFrom(startFrom)
                                                                      .withPartitionKey(String.format("%s#%s", collection.getName(), indexName))
                                                                      .build());
        return new PaginatedResult<>(
                results.getRecords().stream().map(Record::getDocument).collect(Collectors.toList()),
                results.getCursor() != null,
                (String) results.getCursor().getDocument().get(collection.getIdKey()));

    }

    public void deleteDocument(String id, Collection collection) {
        Document document = new Document(collection.getIdKey(), id);
        Record record = convertToRecord(collection, document);
        repository.delete(record.getRecordKey());
    }

}

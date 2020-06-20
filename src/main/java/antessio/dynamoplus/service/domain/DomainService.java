package antessio.dynamoplus.service.domain;

import antessio.dynamoplus.dynamodb.RecordFactory;
import antessio.dynamoplus.dynamodb.bean.Query;
import antessio.dynamoplus.dynamodb.bean.QueryBuilder;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.query.QueryResultsWithCursor;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.bean.PaginatedResult;
import antessio.dynamoplus.service.system.bean.collection.Collection;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DomainService {
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
        return new Document(result);
    }


    public Document createDocument(Document document, Collection collection) {
        Record record = RecordFactory.getInstance().masterRecordFromDocument(document, collection);
        return repository.create(record).getDocument();
    }

    public Document updateDocument(Document document, Collection collection) {
        Record record = RecordFactory.getInstance().masterRecordFromDocument(document, collection);
        return repository.update(record).getDocument();
    }

    public Optional<Document> getDocument(String id, Collection collection) {
        Document document = new Document(collection.getIdKey(), id);
        Record record = RecordFactory.getInstance().masterRecordFromDocument(document, collection);
        return repository.get(record.getPk(), record.getSk())
                .map(Record::getDocument);
    }

    public PaginatedResult<Document> query(
            antessio.dynamoplus.service.bean.Query query,
            Collection collection) {

        Query q = QueryBuilder.aQuery()
                .withPredicate(query.getPredicate())
                .withLimit(query.getLimit())
                .withStartFrom(
                        Optional.ofNullable(query.getLastKey())
                                .flatMap(lastKey -> getDocument(lastKey, collection))
                                .map(lastDocument -> RecordFactory.getInstance().masterRecordFromDocument(lastDocument, collection))
                                .orElse(null)
                )
                .withPartitionKey(String.format("%s#%s", collection.getName(), query.getIndexName()))
                .build();
        QueryResultsWithCursor results = repository.query(q);
        return new PaginatedResult<>(
                results.getRecords().stream().map(Record::getDocument).collect(Collectors.toList()),
                results.getCursor() != null);
    }

    public void deleteDocument(String id, Collection collection) {
        Document document = new Document(collection.getIdKey(), id);
        Record record = RecordFactory.getInstance().masterRecordFromDocument(document, collection);
        repository.delete(record.getPk(), record.getSk());
    }
}

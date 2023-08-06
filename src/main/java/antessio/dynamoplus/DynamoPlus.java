package antessio.dynamoplus;

import java.util.Optional;

import antessio.dynamoplus.common.query.Predicate;
import antessio.dynamoplus.common.query.Query;
import antessio.dynamoplus.security.Authorization;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.bean.PaginatedResult;

public interface DynamoPlus {

    Document createDocument(String collectionName, Document document, Authorization authorization);

    Document updateDocument(String collectionName, String id, Document document, Authorization authorization);

    Optional<Document> getDocument(String collectionName, String id, Authorization authorization);

    void delete(String collectionName, String id, Authorization authorization);

    PaginatedResult<Document, String> query(Query query, Authorization authorization);

}

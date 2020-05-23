package antessio.dynamoplus.dynamodb;

import antessio.dynamoplus.dynamodb.bean.Query;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.query.QueryResultsWithCursor;

import java.util.Optional;

public interface DynamoDbRepository {


    Record create(Record r);

    Optional<Record> get(String pk, String sk);

    Record update(Record r);

    void delete(String pk, String sk);

    QueryResultsWithCursor query(Query query);
}

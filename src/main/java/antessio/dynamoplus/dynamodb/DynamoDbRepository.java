package antessio.dynamoplus.dynamodb;

import antessio.dynamoplus.dynamodb.bean.Query;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.query.QueryResultsWithCursor;

public interface DynamoDbRepository {


    Record create(Record r);

    Record get(String pk, String sk);

    Record update(Record r);

    Record delete(String pk, String sk);

    QueryResultsWithCursor query(Query query);
}

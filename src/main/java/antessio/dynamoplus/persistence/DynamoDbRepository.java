package antessio.dynamoplus.persistence;

import antessio.dynamoplus.persistence.bean.Query;
import antessio.dynamoplus.persistence.bean.Record;
import antessio.dynamoplus.persistence.bean.RecordKey;
import antessio.dynamoplus.persistence.bean.query.QueryResultsWithCursor;

import java.util.Optional;

public interface DynamoDbRepository {


    Record create(Record r);

    Optional<Record> get(String pk, String sk);

    Optional<Record> get(RecordKey recordKey);

    Record update(Record r);

    void delete(RecordKey recordKey);

    QueryResultsWithCursor query(Query query);
    QueryResultsWithCursor query(antessio.dynamoplus.persistence.bean.query.Query query);


}

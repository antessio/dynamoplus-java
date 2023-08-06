package antessio.dynamoplus.persistence.bean.query;

import antessio.dynamoplus.persistence.bean.Record;

import java.util.List;

public class QueryResultsWithCursor {
    private Record cursor;
    private List<Record> records;

    public QueryResultsWithCursor(List<Record> records, Record cursor) {
        this.records = records;
        this.cursor = cursor;
    }

    public Record getCursor() {
        return cursor;
    }

    public List<Record> getRecords() {
        return records;
    }
}

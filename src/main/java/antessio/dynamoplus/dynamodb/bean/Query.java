package antessio.dynamoplus.dynamodb.bean;

import antessio.dynamoplus.dynamodb.bean.query.Predicate;

public class Query {
    private String partitionKey;
    private Predicate predicate;
    private Integer limit;
    private Record startFrom;

    public Query(String partitionKey, Predicate predicate) {
        this.partitionKey = partitionKey;
        this.predicate = predicate;
    }

    public Query(String partitionKey, Predicate predicate, Integer limit, Record startFrom) {
        this.partitionKey = partitionKey;
        this.predicate = predicate;
        this.limit = limit;
        this.startFrom = startFrom;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public Integer getLimit() {
        return limit;
    }

    public Record getStartFrom() {
        return startFrom;
    }
}

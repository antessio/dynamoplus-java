package antessio.dynamoplus.dynamodb.bean;

import antessio.dynamoplus.common.query.Predicate;

public final class QueryBuilder {
    private String partitionKey;
    private Predicate predicate;
    private Integer limit;
    private Record startFrom;

    private QueryBuilder() {
    }

    public static QueryBuilder aQuery() {
        return new QueryBuilder();
    }

    public QueryBuilder withPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

    public QueryBuilder withPredicate(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }

    public QueryBuilder withLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public QueryBuilder withStartFrom(Record startFrom) {
        this.startFrom = startFrom;
        return this;
    }

    public Query build() {
        return new Query(partitionKey, predicate, limit, startFrom);
    }
}

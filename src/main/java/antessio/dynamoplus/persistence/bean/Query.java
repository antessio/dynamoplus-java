package antessio.dynamoplus.persistence.bean;

import antessio.dynamoplus.common.query.Predicate;

import java.util.Objects;

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

    @Override
    public String toString() {
        return "Query{" +
                "partitionKey='" + partitionKey + '\'' +
                ", predicate=" + predicate +
                ", limit=" + limit +
                ", startFrom=" + startFrom +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Query query = (Query) o;
        return Objects.equals(partitionKey, query.partitionKey) &&
                Objects.equals(predicate, query.predicate) &&
                Objects.equals(limit, query.limit) &&
                Objects.equals(startFrom, query.startFrom);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionKey, predicate, limit, startFrom);
    }
}

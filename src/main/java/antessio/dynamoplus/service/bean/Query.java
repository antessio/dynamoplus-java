package antessio.dynamoplus.service.bean;

import antessio.dynamoplus.common.query.Predicate;

import java.util.Objects;

public class Query {
    private String indexName;
    private Integer limit;
    private String lastKey;
    private Predicate predicate;

    public Query(String indexName, Integer limit, String lastKey, Predicate predicate) {
        this.indexName = indexName;
        this.limit = limit;
        this.lastKey = lastKey;
        this.predicate = predicate;
    }

    public String getIndexName() {
        return indexName;
    }

    public Integer getLimit() {
        return limit;
    }

    public String getLastKey() {
        return lastKey;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return "Query{" +
                "indexName='" + indexName + '\'' +
                ", limit=" + limit +
                ", lastKey='" + lastKey + '\'' +
                ", predicate=" + predicate +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Query query = (Query) o;
        return Objects.equals(indexName, query.indexName) &&
                Objects.equals(limit, query.limit) &&
                Objects.equals(lastKey, query.lastKey) &&
                Objects.equals(predicate, query.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, limit, lastKey, predicate);
    }
}

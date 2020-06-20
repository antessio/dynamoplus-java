package antessio.dynamoplus.service.bean;

import antessio.dynamoplus.common.query.Predicate;

public class QueryBuilder {
    private String indexName;
    private Integer limit;
    private String lastKey;
    private Predicate predicate;

    public QueryBuilder indexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public QueryBuilder limit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public QueryBuilder lastKey(String lastKey) {
        this.lastKey = lastKey;
        return this;
    }

    public QueryBuilder predicate(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }

    public Query build() {
        return new Query(indexName, limit, lastKey, predicate);
    }
}
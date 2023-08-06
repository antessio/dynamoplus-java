package antessio.dynamoplus.persistence.bean.query;

import java.util.Optional;

public class Query {


    enum QueryOperator{
        GT,
        GTE,
        LT,
        LTE,
        BEGINS_WITH
    }
    private String indexName;

    public Query(String indexName) {
        this.indexName = indexName;
    }

    public Query() {
    }

    public Optional<String> getIndexName() {
        return Optional.ofNullable(indexName);
    }

}

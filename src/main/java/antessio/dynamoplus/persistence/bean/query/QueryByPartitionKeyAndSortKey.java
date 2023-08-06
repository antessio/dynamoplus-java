package antessio.dynamoplus.persistence.bean.query;

public class QueryByPartitionKeyAndSortKey extends QueryByPartitionKey{

    private String sortKeyGt;
    private QueryOperator operator;


    public QueryByPartitionKeyAndSortKey(String partitionKey, Query.QueryOperator operator) {
        super(partitionKey);
        this.operator = operator;
    }

    public String getSortKeyGt() {
        return sortKeyGt;
    }

    public QueryOperator getOperator() {
        return operator;
    }

}

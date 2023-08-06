package antessio.dynamoplus.persistence.bean.query;

public class QueryByPartitionKeyAndSortKeyBetween extends QueryByPartitionKey{

    private String sortKeyFrom;
    private String sortKeyTo;


    public QueryByPartitionKeyAndSortKeyBetween(String partitionKey, String sortKeyFrom, String sortKeyTo) {
        super(partitionKey);
        this.sortKeyFrom = sortKeyFrom;
        this.sortKeyTo = sortKeyTo;
    }

    public String getSortKeyFrom() {
        return sortKeyFrom;
    }

    public String getSortKeyTo() {
        return sortKeyTo;
    }

}

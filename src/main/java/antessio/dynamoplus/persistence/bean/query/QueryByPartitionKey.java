package antessio.dynamoplus.persistence.bean.query;

public class QueryByPartitionKey extends Query{
    private String partitionKey;

    public QueryByPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

}

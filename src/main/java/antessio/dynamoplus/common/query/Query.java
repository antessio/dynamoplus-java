package antessio.dynamoplus.common.query;

public class Query {

    private final String collectionName;
    private final Predicate predicate;

    private final Integer limit;

    private final String lastId;

    public Query(String collectionName, Predicate predicate, Integer limit, String lastId) {
        this.collectionName = collectionName;
        this.predicate = predicate;
        this.limit = limit;
        this.lastId = lastId;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public Integer getLimit() {
        return limit;
    }

    public String getLastId() {
        return lastId;
    }

}

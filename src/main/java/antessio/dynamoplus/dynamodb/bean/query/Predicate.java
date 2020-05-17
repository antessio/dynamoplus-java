package antessio.dynamoplus.dynamodb.bean.query;

public interface Predicate {
    boolean isRange();

    PredicateValue getValue();
}

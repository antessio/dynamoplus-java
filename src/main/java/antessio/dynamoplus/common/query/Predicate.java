package antessio.dynamoplus.common.query;

public interface Predicate {
    boolean isRange();

    PredicateValue getValue();
}

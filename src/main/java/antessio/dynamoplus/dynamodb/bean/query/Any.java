package antessio.dynamoplus.dynamodb.bean.query;

public class Any implements Predicate {

    public Any() {
    }


    @Override
    public String toString() {
        return "Any{}";
    }

    @Override
    public boolean isRange() {
        return false;
    }

    @Override
    public PredicateValue getValue() {
        return null;
    }
}

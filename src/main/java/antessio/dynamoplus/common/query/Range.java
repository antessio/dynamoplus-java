package antessio.dynamoplus.common.query;

public class Range implements Predicate {
    private String fieldName;
    private String from;
    private String to;

    public Range() {
    }

    public Range(String fieldName, String from, String to) {
        this.fieldName = fieldName;
        this.from = from;
        this.to = to;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    @Override
    public String toString() {
        return "Range{" +
                "fieldName='" + fieldName + '\'' +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                '}';
    }

    @Override
    public boolean isRange() {
        return true;
    }

    @Override
    public PredicateValue getValue() {
        return new PredicateValue(from, to);
    }

}

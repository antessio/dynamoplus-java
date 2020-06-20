package antessio.dynamoplus.common.query;

public class Eq implements Predicate {
    private String fieldName;
    private String fieldValue;

    public Eq() {
    }

    public Eq(String fieldName, String fieldValue) {
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public boolean isRange() {
        return false;
    }

    @Override
    public PredicateValue getValue() {
        return new PredicateValue(fieldValue, null);
    }

    public String getFieldValue() {
        return fieldValue;
    }

    @Override
    public String toString() {
        return "Eq{" +
                "fieldName='" + fieldName + '\'' +
                ", fieldValue='" + fieldValue + '\'' +
                '}';
    }
}

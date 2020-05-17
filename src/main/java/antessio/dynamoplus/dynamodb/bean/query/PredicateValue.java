package antessio.dynamoplus.dynamodb.bean.query;

public class PredicateValue {
    private String value1;
    private String value2;

    public PredicateValue(String value1, String value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    public String getValue1() {
        return value1;
    }

    public String getValue2() {
        return value2;
    }
}

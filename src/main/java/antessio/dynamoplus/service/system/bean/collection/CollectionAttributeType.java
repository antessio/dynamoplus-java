package antessio.dynamoplus.service.system.bean.collection;

public enum CollectionAttributeType {
    STRING(1),
    NUMBER(2),
    OBJECT(3),
    ARRAY(4),
    DATE(5),
    BOOLEAN(6);

    private final int typeCode;

    CollectionAttributeType(int typeCode) {
        this.typeCode = typeCode;
    }

    public int getTypeCode() {
        return typeCode;
    }
}

package antessio.dynamoplus.dynamodb.bean;

import java.util.Map;

public final class RecordBuilder {
    private String pk;
    private String sk;
    private String data;
    private Map<String, Object> document;

    private RecordBuilder() {
    }

    public static RecordBuilder aRecord() {
        return new RecordBuilder();
    }

    public RecordBuilder withPk(String pk) {
        this.pk = pk;
        return this;
    }

    public RecordBuilder withSk(String sk) {
        this.sk = sk;
        return this;
    }

    public RecordBuilder withData(String data) {
        this.data = data;
        return this;
    }

    public RecordBuilder withDocument(Map<String, Object> document) {
        this.document = document;
        return this;
    }

    public Record build() {
        return new Record(pk, sk, data, document);
    }
}

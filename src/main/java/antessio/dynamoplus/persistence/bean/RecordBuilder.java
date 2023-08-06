package antessio.dynamoplus.persistence.bean;

import antessio.dynamoplus.service.bean.Document;

public final class RecordBuilder {

    private RecordKey recordKey;
    private String data;
    private Document document;

    private RecordBuilder() {
    }

    public static RecordBuilder aRecord() {
        return new RecordBuilder();
    }

    public RecordBuilder withRecordKey(RecordKey recordKey) {
        this.recordKey = recordKey;
        return this;
    }

    public RecordBuilder withData(String data) {
        this.data = data;
        return this;
    }

    public RecordBuilder withDocument(Document document) {
        this.document = document;
        return this;
    }

    public Record build() {
        return new Record(recordKey, data, document);
    }

}

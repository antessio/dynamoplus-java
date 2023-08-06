package antessio.dynamoplus.persistence.bean;

import antessio.dynamoplus.service.bean.Document;

public class Record {

    private RecordKey recordKey;
    private String data;
    private Document document;

    public Record() {
    }

    public Record(RecordKey recordKey, String data, Document document) {
        this.recordKey = recordKey;
        this.data = data;
        this.document = document;
    }

    public RecordKey getRecordKey() {
        return recordKey;
    }

    public String getData() {
        return data;
    }

    public Document getDocument() {
        return document;
    }


    @Override
    public String toString() {
        return "Record{" +
               "recordKey=" + recordKey +
               ", data='" + data + '\'' +
               ", document=" + document +
               '}';
    }

}

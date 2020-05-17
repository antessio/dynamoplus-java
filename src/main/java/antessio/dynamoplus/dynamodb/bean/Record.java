package antessio.dynamoplus.dynamodb.bean;

import java.util.Map;

public class Record {
    private String pk;
    private String sk;
    private String data;
    private Map<String, Object> document;

    public Record() {
    }

    public Record(String pk, String sk, String data, Map<String, Object> document) {
        this.pk = pk;
        this.sk = sk;
        this.data = data;
        this.document = document;
    }

    public String getPk() {
        return pk;
    }

    public String getSk() {
        return sk;
    }

    public String getData() {
        return data;
    }

    public Map<String, Object> getDocument() {
        return document;
    }

    @Override
    public String toString() {
        return "Record{" +
                "pk='" + pk + '\'' +
                ", sk='" + sk + '\'' +
                ", data='" + data + '\'' +
                ", document=" + document +
                '}';
    }
}

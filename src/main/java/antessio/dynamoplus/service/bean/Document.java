package antessio.dynamoplus.service.bean;

import antessio.dynamoplus.utils.MapUtil;

import java.util.*;

public class Document {
    private final Map<String, Object> dict;

    private final String id;
    public Document(Map<String, Object> dict, String id) {
        this.dict = Collections.unmodifiableMap(new HashMap<>(dict));
        this.id = id;
    }

    public Document(String idKey, String idValue) {
        this.id = idValue;
        this.dict = Collections.unmodifiableMap(
                new HashMap<>(
                        MapUtil.ofEntries(
                                MapUtil.entry(idKey, idValue)
                        )));
    }

    public Document() {
        this.dict = Collections.emptyMap();
        this.id = null;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Document document = (Document) o;
        return Objects.equals(dict, document.dict);
    }


    public int hashCode() {
        return Objects.hash(dict);
    }


    public String toString() {
        return "Document{" +
                dict +
                '}';
    }

    public Object get(String key) {
        return dict.get(key);
    }

    public Map<String, Object> getDict() {
        return dict;
    }

    public String getId() {
        return id;
    }

}

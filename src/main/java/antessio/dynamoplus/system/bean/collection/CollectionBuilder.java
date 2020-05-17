package antessio.dynamoplus.system.bean.collection;

import java.util.List;

public class CollectionBuilder {
    private String idKey;
    private String name;
    private List<Attribute> attributes;
    private Boolean autoGenerateId;

    public CollectionBuilder idKey(String idKey) {
        this.idKey = idKey;
        return this;
    }

    public CollectionBuilder name(String name) {
        this.name = name;
        return this;
    }

    public CollectionBuilder fields(List<Attribute> attributes) {
        this.attributes = attributes;
        return this;
    }

    public CollectionBuilder autoGenerateId(Boolean autoGenerateId) {
        this.autoGenerateId = autoGenerateId;
        return this;
    }

    public Collection createCollection() {
        return new Collection(idKey, name, attributes, autoGenerateId);
    }
}
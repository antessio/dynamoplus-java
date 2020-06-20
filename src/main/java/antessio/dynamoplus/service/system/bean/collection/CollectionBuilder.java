package antessio.dynamoplus.service.system.bean.collection;

import java.util.List;

public final class CollectionBuilder {
    private String idKey;
    private String name;
    private List<Attribute> attributes;
    private Boolean autoGenerateId;

    private CollectionBuilder() {
    }

    private CollectionBuilder(Collection collection) {
        this.idKey = collection.getIdKey();
        this.name = collection.getName();
        this.attributes = collection.getAttributes();
        this.autoGenerateId = collection.isAutoGenerateId();
    }

    public static CollectionBuilder aCollection() {
        return new CollectionBuilder();
    }

    public static CollectionBuilder aCollection(Collection collection) {
        return new CollectionBuilder(collection);
    }

    public CollectionBuilder withIdKey(String idKey) {
        this.idKey = idKey;
        return this;
    }

    public CollectionBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public CollectionBuilder withAttributes(List<Attribute> attributes) {
        this.attributes = attributes;
        return this;
    }

    public CollectionBuilder withAutoGenerateId(Boolean autoGenerateId) {
        this.autoGenerateId = autoGenerateId;
        return this;
    }

    public Collection build() {
        return new Collection(idKey, name, attributes, autoGenerateId);
    }
}

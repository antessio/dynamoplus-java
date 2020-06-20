package antessio.dynamoplus.service.system.bean.index;

import antessio.dynamoplus.service.system.bean.collection.Collection;

import java.util.List;
import java.util.UUID;

public final class IndexBuilder {
    private UUID uid;
    private String name;
    private Collection collection;
    private List<String> conditions;
    private String orderingKey;

    private IndexBuilder() {
    }

    private IndexBuilder(Index index) {
        this.uid = index.getUid();
        this.name = index.getName();
        this.collection = index.getCollection();
        this.conditions = index.getConditions();
        this.orderingKey = index.getOrderingKey();
    }

    public static IndexBuilder anIndex() {
        return new IndexBuilder();
    }

    public static IndexBuilder anIndex(Index index) {
        return new IndexBuilder(index);
    }

    public IndexBuilder withUid(UUID uid) {
        this.uid = uid;
        return this;
    }

    public IndexBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public IndexBuilder withCollection(Collection collection) {
        this.collection = collection;
        return this;
    }

    public IndexBuilder withConditions(List<String> conditions) {
        this.conditions = conditions;
        return this;
    }

    public IndexBuilder withOrderingKey(String orderingKey) {
        this.orderingKey = orderingKey;
        return this;
    }

    public Index build() {
        return new Index(uid, name, collection, conditions, orderingKey);
    }
}

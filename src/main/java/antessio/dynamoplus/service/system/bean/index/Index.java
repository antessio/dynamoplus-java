package antessio.dynamoplus.service.system.bean.index;


import antessio.dynamoplus.service.system.bean.collection.Collection;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class Index {

    private UUID uid;
    private String name;
    private Collection collection;
    private List<String> conditions;
    private String orderingKey;

    public Index() {
    }


    public Index(UUID uid, String name, Collection collection, List<String> conditions, String orderingKey) {
        this.uid = uid;
        this.name = name;
        this.collection = collection;
        this.conditions = conditions;
        this.orderingKey = orderingKey;
    }

    public Index(String name, Collection collection, List<String> conditions, String orderingKey) {
        this.name = name;
        this.collection = collection;
        this.conditions = conditions;
        this.orderingKey = orderingKey;
    }

    public Index(String name, Collection collection, List<String> conditions) {
        this.name = name;
        this.collection = collection;
        this.conditions = conditions;
        this.orderingKey = null;
    }

    public UUID getUid() {
        return uid;
    }

    public String getName() {
        return name;
    }

    public Collection getCollection() {
        return collection;
    }

    public List<String> getConditions() {
        return conditions;
    }

    public String getOrderingKey() {
        return orderingKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Index index = (Index) o;
        return Objects.equals(uid, index.uid) &&
                Objects.equals(name, index.name) &&
                Objects.equals(collection, index.collection) &&
                Objects.equals(conditions, index.conditions) &&
                Objects.equals(orderingKey, index.orderingKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uid, name, collection, conditions, orderingKey);
    }

    @Override
    public String toString() {
        return "Index{" +
                "uid='" + uid + '\'' +
                ", name='" + name + '\'' +
                ", collection=" + collection +
                ", conditions=" + conditions +
                ", orderingKey='" + orderingKey + '\'' +
                '}';
    }
}

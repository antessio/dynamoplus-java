package antessio.dynamoplus.service.system.bean.collection;

import java.util.List;
import java.util.Objects;

public class Collection {

    private String idKey;
    private String name;
    private List<Attribute> attributes;
    private Boolean autoGenerateId;

    public Collection() {
    }

    public Collection(String idKey, String name, List<Attribute> attributes, Boolean autoGenerateId) {
        this.idKey = idKey;
        this.name = name;
        this.attributes = attributes;
        this.autoGenerateId = autoGenerateId;
    }

    public String getIdKey() {
        return idKey;
    }

    public String getName() {
        return name;
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public Boolean isAutoGenerateId() {
        return autoGenerateId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Collection that = (Collection) o;
        return Objects.equals(idKey, that.idKey) &&
                Objects.equals(name, that.name) &&
                Objects.equals(attributes, that.attributes) &&
                Objects.equals(autoGenerateId, that.autoGenerateId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idKey, name, attributes, autoGenerateId);
    }

    @Override
    public String toString() {
        return "Collection{" +
                "idKey='" + idKey + '\'' +
                ", name='" + name + '\'' +
                ", attributes=" + attributes +
                ", autoGenerateId=" + autoGenerateId +
                '}';
    }
}

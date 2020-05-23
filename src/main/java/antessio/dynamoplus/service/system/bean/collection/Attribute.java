package antessio.dynamoplus.service.system.bean.collection;

import java.util.List;
import java.util.Objects;

public class Attribute {
    private String name;
    private CollectionAttributeType type;
    private List<CollectionAttributeConstraint> constraints;
    private List<Attribute> attributes;

    public Attribute() {
    }

    public Attribute(String name, CollectionAttributeType type, List<CollectionAttributeConstraint> constraints, List<Attribute> attributes) {
        this.name = name;
        this.type = type;
        this.constraints = constraints;
        this.attributes = attributes;
    }

    public String getName() {
        return name;
    }

    public CollectionAttributeType getType() {
        return type;
    }

    public List<CollectionAttributeConstraint> getConstraints() {
        return constraints;
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Attribute attribute = (Attribute) o;
        return Objects.equals(name, attribute.name) &&
                type == attribute.type &&
                Objects.equals(constraints, attribute.constraints) &&
                Objects.equals(attributes, attribute.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, constraints, attributes);
    }

    @Override
    public String toString() {
        return "Attribute{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", constraints=" + constraints +
                ", attributes=" + attributes +
                '}';
    }
}

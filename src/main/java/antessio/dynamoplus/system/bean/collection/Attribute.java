package antessio.dynamoplus.system.bean.collection;

import java.util.List;

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
}

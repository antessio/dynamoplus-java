package antessio.dynamoplus.service.system.bean.collection;

import java.util.List;

public class AttributeBuilder {
    private String attributeName;
    private CollectionAttributeType attributeType;
    private List<CollectionAttributeConstraint> constraints;
    private List<Attribute> attributes;

    public AttributeBuilder attributeName(String attributeName) {
        this.attributeName = attributeName;
        return this;
    }

    public AttributeBuilder attributeType(CollectionAttributeType attributeType) {
        this.attributeType = attributeType;
        return this;
    }

    public AttributeBuilder constraints(List<CollectionAttributeConstraint> constraints) {
        this.constraints = constraints;
        return this;
    }

    public AttributeBuilder attributes(List<Attribute> attributes) {
        this.attributes = attributes;
        return this;
    }

    public Attribute build() {
        return new Attribute(attributeName, attributeType, constraints, attributes);
    }
}
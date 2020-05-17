package antessio.dynamoplus.service;

import antessio.dynamoplus.system.bean.collection.*;


import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static antessio.dynamoplus.utils.DynamoPlusUtils.safeGet;
import static antessio.dynamoplus.utils.MapUtil.entry;
import static antessio.dynamoplus.utils.MapUtil.ofEntries;
import static java.util.stream.Collectors.toList;

public class CollectionService {

    public static final String NAME = "name";
    public static final String ID_KEY = "id_key";
    public static final String ATTRIBUTES = "attributes";
    public static final String ATTRIBUTE_NAME = "name";
    public static final String ATTRIBUTE_TYPE = "type";
    public static final String ATTRIBUTE_SUB_ATTRIBUTES = "attributes";
    public static final String ATTRIBUTE_CONSTRAINTS = "constraints";
    public static final String AUTO_GENERATE_ID = "auto_generate_id";

    public static Map<String, Object> fromCollectionToMapMin(Collection collection) {
        return ofEntries(
                entry(NAME, collection.getName()),
                entry(ID_KEY, collection.getIdKey())
        );
    }

    public static Map<String, Object> fromCollectionToMap(Collection collection) {
        return Stream.concat(
                fromCollectionToMapMin(collection).entrySet().stream(),
                ofEntries(
                        entry(AUTO_GENERATE_ID, collection.isAutoGenerateId() + ""),
                        entry(ATTRIBUTES, collection.getAttributes().stream().map(CollectionService::fromAttributeToMap).collect(toList()))
                ).entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, Object> fromAttributeToMap(Attribute attribute) {
        return ofEntries(
                entry(ATTRIBUTE_NAME, attribute.getName()),
                entry(ATTRIBUTE_TYPE, attribute.getType().name()),
                entry(ATTRIBUTE_CONSTRAINTS, attribute.getConstraints().stream().map(CollectionAttributeConstraint::name).collect(toList())),
                entry(ATTRIBUTE_SUB_ATTRIBUTES, attribute.getAttributes().stream().map(CollectionService::fromAttributeToMap).collect(toList()))

        );
    }

    public static Collection fromMapToCollection(Map<String, Object> document) {

        return new CollectionBuilder()
                .autoGenerateId(Boolean.valueOf(safeGet(document, String.class, AUTO_GENERATE_ID)))
                .idKey(safeGet(document, String.class, ID_KEY))
                .name(safeGet(document, String.class, NAME))
                .fields(convertToAttributeList(document))
                .createCollection();
    }

    private static List<Attribute> convertToAttributeList(Map<String, Object> document) {
        return document.containsKey(ATTRIBUTES) ?
                (List<Attribute>) Optional.ofNullable(safeGet(document, List.class, ATTRIBUTES))
                        .map(l -> l.stream()
                                .filter(e -> Map.class.isAssignableFrom(e.getClass()))
                                .map(e -> fromMapToAttribute(((Map<String, Object>) e)))
                                .collect(toList()))
                        .orElse(null) :
                Collections.emptyList()
                ;
    }

    private static Attribute fromMapToAttribute(Map<String, Object> a) {
        return new AttributeBuilder()
                .attributeName(safeGet(a, String.class, ATTRIBUTE_NAME))
                .attributeType(CollectionAttributeType.valueOf(safeGet(a, String.class, ATTRIBUTE_TYPE)))
                .constraints(a.containsKey(ATTRIBUTE_CONSTRAINTS) ? convertToConstraintList(a) : null)
                .attributes(a.containsKey(ATTRIBUTES) ? convertToAttributeList(a) : null)
                .build();
    }

    private static List<CollectionAttributeConstraint> convertToConstraintList(Map<String, Object> a) {
        return (List<CollectionAttributeConstraint>) safeGet(a, List.class, ATTRIBUTE_CONSTRAINTS)
                .stream()
                .filter(e -> e instanceof String)
                .map(e -> CollectionAttributeConstraint.valueOf((String) e))
                .collect(toList());
    }
}

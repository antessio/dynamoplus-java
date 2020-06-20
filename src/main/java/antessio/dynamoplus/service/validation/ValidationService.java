package antessio.dynamoplus.service.validation;


import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.exception.DocumentValidationException;
import antessio.dynamoplus.service.system.ClientAuthorizationService;
import antessio.dynamoplus.service.system.CollectionService;
import antessio.dynamoplus.service.system.IndexService;
import antessio.dynamoplus.service.system.bean.collection.Attribute;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.collection.CollectionAttributeConstraint;
import antessio.dynamoplus.utils.ConversionUtils;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class ValidationService {

    private static final String COLLECTION_ATTRIBUTE_BASE_SCHEMA = "{" +
            "    \"name\": {\"type\": \"string\"}," +
            "    \"type\": {\"type\": \"string\", \"enum\": [\"STRING\", \"OBJECT\", \"NUMBER\", \"DATE\", \"ARRAY\"]}," +
            "    \"constraints\": {\"type\": \"array\", \"items\": {\"type\":\"string\",\"enum\": [\"NULLABLE\", \"NOT_NULL\"]}}" +
            "}";
    private static final String COLLECTION_ATTRIBUTE_SCHEMA = "{" +
            "    \"properties\": {" +
            "        §COLLECTION_ATTRIBUTE_BASE_SCHEMA_DEFINITION§," +
            "        \"attributes\": {" +
            "            \"type\": \"array\"," +
            "            \"items\": {\"properties\": §COLLECTION_ATTRIBUTE_BASE_SCHEMA_DEFINITION§ }" +
            "        }" +
            "    }," +
            "    \"required\": [\"name\", \"type\"]" +
            "}";
    private final String COLLECTION_SCHEMA = "{" +
            "  \"$schema\": \"http://json-schema.org/draft-04/schema#\"," +
            "  \"title\": \"collection\"," +
            "  \"type\": \"object\"," +
            "  \"properties\": {" +
            "    \"id_key\": {\"type\": \"string\"}, " +
            "      \"name\": {\"type\": \"string\"}, " +
            "        \"ordering\": {\"type\": \"string\"}, " +
            "          \"auto_generate_id\": {\"type\": \"boolean\"}, " +
            "            \"attributes\": {" +
            "              \"type\": \"array\", " +
            "                \"items\": {" +
            "                  \"properties\": {\"name\": {\"type\": \"string\"}, " +
            "                    \"type\": {\"type\": \"string\", \"enum\": [\"STRING\", \"OBJECT\", \"NUMBER\", \"DATE\", \"ARRAY\"]}, " +
            "                      \"constraints\": {" +
            "                        \"type\": \"array\", " +
            "                          \"items\": {" +
            "                            \"type\": \"string\", \"enum\": [\"NULLABLE\", \"NOT_NULL\"]}}, " +
            "                              \"attributes\": {" +
            "                                \"type\": \"array\", " +
            "                                  \"items\": {" +
            "                                    \"properties\": {" +
            "                                      \"name\": {\"type\": \"string\"}, \"type\": {\"type\": \"string\", \"enum\": [\"STRING\", \"OBJECT\", \"NUMBER\", \"DATE\", \"ARRAY\"]}, \"constraints\": {" +
            "                                        \"type\": \"array\", " +
            "                                          \"items\": {\"type\": \"string\", \"enum\": [\"NULLABLE\", \"NOT_NULL\"]}" +
            "                                      }" +
            "                                    }" +
            "                                  }" +
            "                              }" +
            "                                }, " +
            "                                  \"required\": [\"name\", \"type\"]" +
            "                }" +
            "            }" +
            "  }, " +
            "    \"required\": [\"id_key\", \"name\"]" +
            "}";

    private ValidationService() {
    }

    static ValidationService instance;

    public static ValidationService getInstance() {
        if (instance == null) {
            instance = new ValidationService();
        }
        return instance;
    }

    public void validate(Document document, Collection collection) {
        List<String> requiredFields = new ArrayList<>();
        if (collection.isAutoGenerateId() == null || !collection.isAutoGenerateId()) {
            requiredFields.add(collection.getIdKey());
        }
        JSONObject propertiesSchema = new JSONObject();
        if (collection.getAttributes() != null) {
            requiredFields.addAll(getRequiredAttributes(collection.getAttributes()));
            for (Attribute a : collection.getAttributes()) {
                propertiesSchema.put(a.getName(), fromAttributeToJsonSchema(a));
            }
        }
        JSONObject baseSchema = new JSONObject()
                .put("$schema", "http://json-schema.org/draft-04/schema#")
                .put("type", "object")
                .put("name", collection.getName())
                .put("properties", propertiesSchema)
                .put("required", requiredFields);
        Schema schema = SchemaLoader.builder()
                .useDefaults(true)
                .schemaJson(baseSchema)
                .build()
                .load().build();
        JSONObject jsonObject = new JSONObject(ConversionUtils.getInstance().convertToJson(document));
        try {
            schema.validate(jsonObject);
        } catch (ValidationException e) {
            throw new DocumentValidationException(document, e.getAllMessages(), collection, e);
        }


    }

    private JSONObject fromAttributeToJsonSchema(Attribute attribute) {
        JSONObject properties = new JSONObject();
        switch (attribute.getType()) {
            case NUMBER:
                properties.put("type", "number");
                break;
            case OBJECT:
                properties.put("type", "object");
                properties.put("properties", getSchemaForObject(attribute));
                properties.put("required", getRequiredAttributes(attribute.getAttributes()));
                break;
            case ARRAY:
                properties.put("type", "array");

                JSONObject items = getSchemaForArray(attribute);
                properties.put("items", items);
                break;
            default:
                properties.put("type", "string");
                break;
        }

        return properties;
    }

    private JSONObject getSchemaForArray(Attribute attribute) {
        JSONObject items = new JSONObject();
        if (attribute.getAttributes() != null) {
            JSONObject ps = new JSONObject();
            for (Attribute a : attribute.getAttributes()) {
                ps.put(a.getName(), fromAttributeToJsonSchema(a));
            }
            items.put("properties", ps);
        }
        return items;
    }

    private JSONObject getSchemaForObject(Attribute attribute) {
        JSONObject propertiesSchema = new JSONObject();
        if (attribute.getAttributes() != null) {
            for (Attribute a : attribute.getAttributes()) {
                propertiesSchema.put(a.getName(), fromAttributeToJsonSchema(a));
            }
        }
        return propertiesSchema;
    }


    private List<String> getRequiredAttributes(List<Attribute> attributes) {
        return attributes.stream()
                .filter(a -> a.getConstraints() != null &&
                        a.getConstraints()
                                .stream()
                                .anyMatch(c -> c == CollectionAttributeConstraint.NOT_NULL)
                )
                .map(Attribute::getName)
                .collect(Collectors.toList());

    }

    public void validateCollection(Document document) {
        validate(document, CollectionService.COLLECTION_METADATA);

    }

    public void validateClientAuthorization(Document document) {
        validate(document, ClientAuthorizationService.CLIENT_AUTHORIZATION_METADATA);
    }

    public void validateIndex(Document document) {
        validate(document, IndexService.INDEX_COLLECTION);
    }
}

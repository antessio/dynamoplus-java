package antessio.dynamoplus.service.validation;

import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.exception.DocumentValidationException;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationApiKeyBuilder;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationScope;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationScopeBuilder;
import antessio.dynamoplus.service.system.bean.collection.*;
import antessio.dynamoplus.utils.ConversionUtils;
import antessio.dynamoplus.utils.MapUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ValidationServiceTest {
    private ValidationService validationService;

    @BeforeEach
    void setUp() {
        validationService = ValidationService.getInstance();
    }

    //================================== collection =============================
    @Test
    void testCollectionValidationBasic() {
        //given
        Document collection = ConversionUtils.getInstance()
                .convertObject(CollectionBuilder.aCollection()
                        .withName("example")
                        .withIdKey("id")
                        .build());
        //when
        validationService.validateCollection(collection);
        //then
    }

    @Test
    void testCollectionValidationWithAttributes() {
        //given
        Document collection = ConversionUtils.getInstance()
                .convertObject(CollectionBuilder.aCollection()
                        .withName("example")
                        .withIdKey("id")
                        .withAttributes(Arrays.asList(
                                new AttributeBuilder()
                                        .attributeType(CollectionAttributeType.STRING)
                                        .attributeName("name")
                                        .build(),
                                new AttributeBuilder()
                                        .attributeType(CollectionAttributeType.NUMBER)
                                        .attributeName("category")
                                        .build(),
                                new AttributeBuilder()
                                        .attributeType(CollectionAttributeType.OBJECT)
                                        .attributeName("metadata")
                                        .build()
                        ))
                        .build());
        //when
        validationService.validateCollection(collection);
        //then
    }

    @Test
    void testCollectionValidationEmpty() {
        //given
        //when
        //then
        assertThatExceptionOfType(DocumentValidationException.class)
                .isThrownBy(() -> validationService.validateCollection(new Document()))
                .matches(e -> e.getAllMessages().equals(
                        Arrays.asList(
                                "#: required key [name] not found",
                                "#: required key [id_key] not found"
                        )
                ));

    }


    //================================== client authorization =============================


    @Test
    void testClientAuthorizationValidationBasic() {
        //given
        Document clientAuthorization = ConversionUtils.getInstance()
                .convertObject(
                        ClientAuthorizationApiKeyBuilder.aClientAuthorizationApiKey()
                                .withClientId("test")
                                .withClientScopes(Arrays.asList(
                                        ClientAuthorizationScopeBuilder.aClientAuthorizationScope()
                                                .withScopeType(ClientAuthorizationScope.ScopeType.CREATE)
                                                .withCollectionName("example")
                                                .build(),
                                        ClientAuthorizationScopeBuilder.aClientAuthorizationScope()
                                                .withScopeType(ClientAuthorizationScope.ScopeType.DELETE)
                                                .withCollectionName("example")
                                                .build()
                                ))
                                .withApiKey("abcdefg")
                                .withWhiteListHosts(Collections.singletonList("http://localhost:8000"))
                                .build()
                );
        //when
        //then
        validationService.validateClientAuthorization(clientAuthorization);
    }

    //================================== document =============================
    @Test
    void testDocumentValidationBasic() {
        //given
        Collection collection = CollectionBuilder.aCollection()
                .withName("example")
                .withIdKey("id")
                .withAttributes(Arrays.asList(
                        new AttributeBuilder()
                                .attributeName("name")
                                .attributeType(CollectionAttributeType.STRING)
                                .build(),
                        new AttributeBuilder()
                                .attributeName("value")
                                .attributeType(CollectionAttributeType.NUMBER)
                                .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                                .build(),
                        new AttributeBuilder()
                                .attributeName("child")
                                .attributeType(CollectionAttributeType.OBJECT)
                                .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                                .attributes(Arrays.asList(
                                        new AttributeBuilder()
                                                .attributeType(CollectionAttributeType.STRING)
                                                .attributeName("test")
                                                .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                                                .build(),
                                        new AttributeBuilder()
                                                .attributeType(CollectionAttributeType.STRING)
                                                .attributeName("test2")
                                                .constraints(Collections.singletonList(CollectionAttributeConstraint.NULLABLE))
                                                .build()

                                ))
                                .build()

                ))
                .build();
        Document expectedDocument = new Document();
        //when
        //then
        assertThatExceptionOfType(DocumentValidationException.class)
                .isThrownBy(() -> {
                    validationService.validate(expectedDocument, collection);
                })
                .matches(e -> e.getDocument().equals(expectedDocument))
                .matches(e -> e.getCollection().equals(collection))
                .matches(e -> e.getAllMessages().equals(
                        Arrays.asList(
                                "#: required key [id] not found",
                                "#: required key [value] not found",
                                "#: required key [child] not found"
                        )
                ));
    }

    @Test
    void testDocumentValidationNestedFields() {
        //given
        Collection collection = CollectionBuilder.aCollection()
                .withName("example")
                .withIdKey("id")
                .withAttributes(Arrays.asList(
                        new AttributeBuilder()
                                .attributeName("name")
                                .attributeType(CollectionAttributeType.STRING)
                                .build(),
                        new AttributeBuilder()
                                .attributeName("value")
                                .attributeType(CollectionAttributeType.NUMBER)
                                .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                                .build(),
                        new AttributeBuilder()
                                .attributeName("child")
                                .attributeType(CollectionAttributeType.OBJECT)
                                .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                                .attributes(Arrays.asList(
                                        new AttributeBuilder()
                                                .attributeType(CollectionAttributeType.STRING)
                                                .attributeName("test")
                                                .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                                                .build(),
                                        new AttributeBuilder()
                                                .attributeType(CollectionAttributeType.STRING)
                                                .attributeName("test2")
                                                .constraints(Collections.singletonList(CollectionAttributeConstraint.NULLABLE))
                                                .build(),
                                        new AttributeBuilder()
                                                .attributeType(CollectionAttributeType.OBJECT)
                                                .attributeName("son_of_child")
                                                .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                                                .attributes(Arrays.asList(
                                                        new AttributeBuilder()
                                                                .attributeType(CollectionAttributeType.STRING)
                                                                .attributeName("test3")
                                                                .build(),
                                                        new AttributeBuilder()
                                                                .attributeType(CollectionAttributeType.ARRAY)
                                                                .attributeName("test4")

                                                                .build()
                                                ))
                                                .build()

                                ))
                                .build()

                ))
                .build();
        Document expectedDocument = new Document(
                MapUtil.ofEntries(
                        MapUtil.entry("child", MapUtil.ofEntries(
                                MapUtil.entry("test2", true),
                                MapUtil.entry("son_of_child", MapUtil.ofEntries(
                                        MapUtil.entry("test3", new HashMap<>()),
                                        MapUtil.entry("test4", "test")
                                ))
                        ))
                )
        );
        //when
        //then
        assertThatExceptionOfType(DocumentValidationException.class)
                .isThrownBy(() -> validationService.validate(expectedDocument, collection))
                .matches(e -> e.getDocument().equals(expectedDocument))
                .matches(e -> e.getCollection().equals(collection))
                .matches(e -> e.getAllMessages().equals(
                        Arrays.asList(
                                "#: required key [id] not found",
                                "#: required key [value] not found",
                                "#/child: required key [test] not found",
                                "#/child/test2: expected type: String, found: Boolean",
                                "#/child/son_of_child/test4: expected type: JSONArray, found: String",
                                "#/child/son_of_child/test3: expected type: String, found: JSONObject"
                        )
                ));
    }
}

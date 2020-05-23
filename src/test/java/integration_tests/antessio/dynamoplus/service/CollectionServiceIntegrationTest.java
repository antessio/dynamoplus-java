package integration_tests.antessio.dynamoplus.service;

import antessio.dynamoplus.dynamodb.DynamoDb;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.system.CollectionService;
import antessio.dynamoplus.service.system.bean.collection.AttributeBuilder;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.collection.CollectionAttributeType;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.assertj.core.internal.FieldByFieldComparator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;


import static org.assertj.core.api.Assertions.assertThat;

public class CollectionServiceIntegrationTest extends IntegrationTest {
    public static final String COLLECTION_NAME = "test";
    private CollectionService collectionService;


    @BeforeEach
    void setUp() {
        AmazonDynamoDB client = dynamoLocal();
        DynamoDb dynamoDb = new DynamoDb(client);
        DynamoDbTableRepository tableRepository = new DynamoDbTableRepository(dynamoDb, "system");
        collectionService = new CollectionService(tableRepository);
    }

    @Test
    @Order(0)
    void testInsertAndLoadIndex() {
        //given
        Collection toInsert = new CollectionBuilder()
                .idKey("id")
                .name(COLLECTION_NAME)
                .attributes(Arrays.asList(
                        new AttributeBuilder()
                                .attributeName("field1")
                                .attributeType(CollectionAttributeType.STRING)
                                .build(),
                        new AttributeBuilder()
                                .attributeName("field2")
                                .attributeType(CollectionAttributeType.NUMBER)
                                .build()
                ))
                .createCollection();
        //when
        Collection created = collectionService.createCollection(toInsert);
        //then
        assertThat(created)
                .isNotNull();
        Optional<Collection> loaded = collectionService.getCollectionByName(created.getName());
        assertThat(loaded)
                .get()
                .usingComparator(new FieldByFieldComparator())
                .isEqualToIgnoringGivenFields(created, "attributes");
        assertThat(loaded.get().getAttributes())
                .usingComparator(new FieldByFieldComparator())
                .isEqualTo(created.getAttributes());

    }


    @Order(1)
    @Test
    public void testUpdate() {
        Collection toUpdate = new CollectionBuilder()
                .idKey("id")
                .name(COLLECTION_NAME)
                .attributes(Arrays.asList(
                        new AttributeBuilder()
                                .attributeName("field1")
                                .attributeType(CollectionAttributeType.NUMBER)
                                .build(),
                        new AttributeBuilder()
                                .attributeName("field3")
                                .attributeType(CollectionAttributeType.NUMBER)
                                .build()
                ))
                .createCollection();

        Collection updated = collectionService.updateCollection(toUpdate);
        //then
        assertThat(updated)
                .isNotNull();
        Optional<Collection> loaded = collectionService.getCollectionByName(updated.getName());
        assertThat(loaded)
                .get()
                .usingComparator(new FieldByFieldComparator())
                .isEqualToIgnoringGivenFields(updated, "attributes");
        assertThat(loaded.get().getAttributes())
                .usingComparator(new FieldByFieldComparator())
                .isEqualTo(updated.getAttributes());
    }

    @Order(2)
    @Test
    void testDelete() {
        //given
        //when
        collectionService.delete(COLLECTION_NAME);
        //then
        collectionService.getCollectionByName(COLLECTION_NAME);
    }
}

package integration_tests.antessio.dynamoplus.service;

import antessio.dynamoplus.dynamodb.DynamoDb;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.CollectionService;
import antessio.dynamoplus.system.bean.collection.*;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.assertj.core.internal.FieldByFieldComparator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class CollectionServiceIntegrationTest extends SystemIntegrationTest {
    private CollectionService collectionService;


    @BeforeEach
    void setUp() {
        AmazonDynamoDB client = dynamoLocal();
        DynamoDb dynamoDb = new DynamoDb(client, "system");
        DynamoDbTableRepository tableRepository = new DynamoDbTableRepository(dynamoDb);
        collectionService = new CollectionService(tableRepository);
    }

    @Test
    void testInsertAndLoadIndex() {
        //given
        Collection toInsert = new CollectionBuilder()
                .idKey("id")
                .name("test")
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
        Collection loaded = collectionService.getCollectionByName(created.getName());
        assertThat(loaded)
                .usingComparator(new FieldByFieldComparator())
                .isEqualToIgnoringGivenFields(created, "attributes");
        assertThat(loaded.getAttributes())
                .usingComparator(new FieldByFieldComparator())
                .isEqualTo(created.getAttributes());

    }
}

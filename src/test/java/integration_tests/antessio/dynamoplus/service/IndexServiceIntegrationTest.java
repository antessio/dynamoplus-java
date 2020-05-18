package integration_tests.antessio.dynamoplus.service;

import antessio.dynamoplus.dynamodb.DynamoDb;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.IndexService;
import antessio.dynamoplus.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.system.bean.index.Index;
import antessio.dynamoplus.system.bean.index.IndexBuilder;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import org.assertj.core.internal.FieldByFieldComparator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class IndexServiceIntegrationTest {
    private IndexService indexService;

    private static AmazonDynamoDB dynamoLocal() {
        String endpoint = "http://localhost:9898";
        BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");
        AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(endpoint, null)
                );
        return clientBuilder.build();
    }

    @BeforeAll
    static void beforeAll() {
        dynamoLocal().createTable(new CreateTableRequest()
                .withTableName("system")
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1l).withWriteCapacityUnits(1l))
                .withAttributeDefinitions(new AttributeDefinition()
                                .withAttributeName("pk")
                                .withAttributeType(ScalarAttributeType.S),
                        new AttributeDefinition()
                                .withAttributeName("sk")
                                .withAttributeType(ScalarAttributeType.S),
                        new AttributeDefinition()
                                .withAttributeName("data")
                                .withAttributeType(ScalarAttributeType.S)
                )
                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("sk-data-index")
                        .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1l).withWriteCapacityUnits(1l))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                        .withKeySchema(
                                new KeySchemaElement().withAttributeName("sk")
                                        .withKeyType(KeyType.HASH),
                                new KeySchemaElement().withAttributeName("data")
                                        .withKeyType(KeyType.RANGE)))
                .withKeySchema(
                        new KeySchemaElement().withAttributeName("pk")
                                .withKeyType(KeyType.HASH),
                        new KeySchemaElement().withAttributeName("sk")
                                .withKeyType(KeyType.RANGE))
        );
    }

    @AfterAll
    static void afterAll() {
        dynamoLocal().deleteTable("system");
    }

    @BeforeEach
    void setUp() {
        AmazonDynamoDB client = dynamoLocal();

        DynamoDb dynamoDb = new DynamoDb(client, "system");
        DynamoDbTableRepository tableRepository = new DynamoDbTableRepository(dynamoDb);
        indexService = new IndexService(tableRepository);
    }

    @Test
    void testInsertAndLoadIndex() {
        //given
        Index toInsert = new IndexBuilder()
                .uid(UUID.randomUUID())
                .collection(new CollectionBuilder()
                        .idKey("id")
                        .name("test")
                        .createCollection())
                .conditions(Collections.singletonList("name"))
                .createIndex();
        //when
        Index indexCreated = indexService.createIndex(toInsert);
        //then
        assertThat(indexCreated)
                .isNotNull();
        Index loaded = indexService.getById(indexCreated.getUid());
        assertThat(loaded)
                .usingComparator(new FieldByFieldComparator())
                .isEqualToIgnoringGivenFields(indexCreated, "collection");

    }
}

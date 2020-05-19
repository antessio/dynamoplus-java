package integration_tests.antessio.dynamoplus.service;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class SystemIntegrationTest {
    static AmazonDynamoDB dynamoLocal() {
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
        try {
            dynamoLocal().deleteTable("system");
        } catch (ResourceNotFoundException e) {

        }
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


}

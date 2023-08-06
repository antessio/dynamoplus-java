package integration_tests.antessio.dynamoplus.service;


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.dynamodb.DynaliteContainer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import antessio.dynamoplus.BaseUnitTest;

public class IntegrationTest extends BaseUnitTest {
    final static Logger logger = LoggerFactory.getLogger(IntegrationTest.class);
    protected static DynaliteContainer dynamoDBTestContainer = new DynaliteContainer();
    protected static AmazonDynamoDB dynamoLocal() {

        return dynamoDBTestContainer.getClient();
    }

    @BeforeAll
    static void beforeAll() {
        dynamoDBTestContainer.start();
        deleteTable("system");
        deleteTable("domain");
        createTable("system");
        createTable("domain");
        checkStarted();


    }

    private static void checkStarted() {
        int retries = 0;
        while (!checkTableExists() || retries >= 3){
            logger.info("waiting for container to start");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            retries++;
        }
        if (!checkTableExists()){
            throw new IllegalStateException("container not started");
        }
    }

    private static boolean checkTableExists() {
        try {
            TableDescription systemTable = dynamoLocal().describeTable("system").getTable();
            TableDescription domainTable = dynamoLocal().describeTable("domain").getTable();
            return systemTable != null && domainTable != null;
        }catch (Exception e){
            return false;
        }
    }

    @AfterAll
    static void afterAll() {
        dynamoDBTestContainer.stop();
    }

    private static void createTable(String tableName) {
        dynamoLocal().createTable(new CreateTableRequest()
                .withTableName(tableName)
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

    private static void deleteTable(String tableName) {
        try {
            dynamoLocal().deleteTable(tableName);
        } catch (ResourceNotFoundException e) {

        }
    }


}

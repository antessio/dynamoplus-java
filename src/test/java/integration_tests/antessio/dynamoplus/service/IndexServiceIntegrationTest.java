package integration_tests.antessio.dynamoplus.service;

import antessio.dynamoplus.dynamodb.DynamoDb;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.system.IndexService;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.service.system.bean.index.Index;
import antessio.dynamoplus.service.system.bean.index.IndexBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.assertj.core.internal.FieldByFieldComparator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class IndexServiceIntegrationTest extends IntegrationTest {
    private IndexService indexService;


    @BeforeEach
    void setUp() {
        AmazonDynamoDB client = dynamoLocal();

        DynamoDb dynamoDb = new DynamoDb(client);
        DynamoDbTableRepository tableRepository = new DynamoDbTableRepository(dynamoDb, "system");
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
        Optional<Index> loaded = indexService.getById(indexCreated.getUid());
        assertThat(loaded)
                .get()
                .usingComparator(new FieldByFieldComparator())
                .isEqualToIgnoringGivenFields(indexCreated, "collection");

    }
}

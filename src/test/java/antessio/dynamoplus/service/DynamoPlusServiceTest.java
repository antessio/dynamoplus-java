package antessio.dynamoplus.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import antessio.dynamoplus.DynamoPlus;
import antessio.dynamoplus.persistence.DynamoDb;
import antessio.dynamoplus.persistence.impl.DynamoDbTableRepository;
import antessio.dynamoplus.security.Authorization;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.domain.DomainService;
import antessio.dynamoplus.service.system.ClientAuthorizationService;
import antessio.dynamoplus.service.system.CollectionService;
import antessio.dynamoplus.service.system.IndexService;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationInterface;
import antessio.dynamoplus.service.validation.ValidationService;
import antessio.dynamoplus.utils.MapUtil;
import integration_tests.antessio.dynamoplus.service.IntegrationTest;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DynamoPlusServiceTest extends IntegrationTest {

    final Authorization MOCKED_VERIFIED_AUTHORIZATION = new MockedAuthorization("test", true);

    private DynamoPlus service;

    @BeforeEach
    protected void setUp() {
        super.setUp();
        AmazonDynamoDB client = dynamoDBTestContainer.getClient();
        DynamoDb dynamoDbClient = new DynamoDb(client);
        IndexService indexService = new IndexService(new DynamoDbTableRepository(dynamoDbClient, "system"));
        CollectionService collectionService = new CollectionService(new DynamoDbTableRepository(dynamoDbClient, "system"));
        ClientAuthorizationService clientAuthorizationService = new ClientAuthorizationService(new DynamoDbTableRepository(dynamoDbClient, "system"));
        DomainService domainService = new DomainService(new DynamoDbTableRepository(dynamoDbClient, "domain"));
        ValidationService validationService = ValidationService.getInstance();
        service = new DynamoPlusService(
                indexService,
                collectionService,
                clientAuthorizationService,
                domainService,
                validationService,
                false);


    }

    @Order(1)
    @Test
    void testCreateCollectionBasic() {
        //given
        String expectedCollectionName = "collection";
        Document expectedDocument = new Document(
                MapUtil.ofEntries(
                        MapUtil.entry("id_key", "id"),
                        MapUtil.entry("name", "example")
                ),
                id);

        //when
        Document document = service.createDocument(expectedCollectionName, expectedDocument, MOCKED_VERIFIED_AUTHORIZATION);
        //then
        assertThat(document).isEqualTo(expectedDocument);

    }

    @Order(2)
    @Test
    void testCreateIndex() {
        //given
        String expectedCollectionName = "index";
        Document expectedDocument = randomIndexDocument();

        //when
        Document document = service.createDocument(expectedCollectionName, expectedDocument, MOCKED_VERIFIED_AUTHORIZATION);
        //then
        assertThat(document).isEqualTo(expectedDocument);

    }


    @Order(3)
    @Test
    void testCreateDocument() {

        //given
        String expectedCollectionName = "example";
        String expectedId = UUID.randomUUID().toString();
        Document expectedDocument = new Document(
                MapUtil.ofEntries(
                        MapUtil.entry("id", expectedId),
                        MapUtil.entry("key", "value")
                ),
                id);

        //when
        Document createDocumentResult = service.createDocument(expectedCollectionName, expectedDocument, MOCKED_VERIFIED_AUTHORIZATION);
        Optional<Document> getDocumentResult = service.getDocument(expectedCollectionName, expectedId, MOCKED_VERIFIED_AUTHORIZATION);
        //then
        assertThat(createDocumentResult).isEqualTo(expectedDocument);
        assertThat(getDocumentResult).get().isEqualTo(createDocumentResult);

    }


    @Test
    @Order(4)
    void testClientAuthorization() {
        //given
        String expectedCollectionName = "client_authorization";
        Document expectedDocument = randomClientAuthorizationDocument();

        //when
        Document document = service.createDocument(expectedCollectionName, expectedDocument, MOCKED_VERIFIED_AUTHORIZATION);
        //then
        assertThat(document).isEqualTo(expectedDocument);

    }

    @Test
    @Order(5)
    void updateDocument() {
        //given
        String expectedCollectionName = "example";
        String expectedId = UUID.randomUUID().toString();
        Document expectedDocument = new Document(
                MapUtil.ofEntries(
                        MapUtil.entry("id", expectedId),
                        MapUtil.entry("key", "value")
                ),
                id);

        Document documentToUpdate = new Document(
                MapUtil.merge(
                        expectedDocument.getDict(),
                        Map.of("attribute1", "field1")),

                id);
        //when
        Document createdDocument = service.createDocument(expectedCollectionName, expectedDocument, MOCKED_VERIFIED_AUTHORIZATION);
        Document updatedDocument = service.updateDocument(expectedCollectionName, expectedId, documentToUpdate, MOCKED_VERIFIED_AUTHORIZATION);
        //then
        assertThat(updatedDocument).isEqualTo(documentToUpdate);
        assertThat(createdDocument).isNotEqualTo(updatedDocument);

    }


    @Test
    @Order(6)
    void delete() {
        //given
        String expectedCollectionName = "example";
        String expectedId = UUID.randomUUID().toString();
        Document expectedDocument = new Document(
                MapUtil.ofEntries(
                        MapUtil.entry("id", expectedId),
                        MapUtil.entry("key", "value")
                ),
                id);
        //when
        Document createdDocument = service.createDocument(expectedCollectionName, expectedDocument, MOCKED_VERIFIED_AUTHORIZATION);
        service.delete(expectedCollectionName, expectedId, MOCKED_VERIFIED_AUTHORIZATION);
        Optional<Document> maybeDocument = service.getDocument(expectedCollectionName, expectedId, MOCKED_VERIFIED_AUTHORIZATION);
        //then
        assertThat(maybeDocument).isEmpty();
    }

    @Test
    void query() {
        //given
        //when
        //then
    }

    static class MockedAuthorization extends Authorization {

        private final boolean isVerified;

        protected MockedAuthorization(String clientId, boolean isVerified) {
            super(clientId);
            this.isVerified = isVerified;
        }

        @Override
        public boolean verify(ClientAuthorizationInterface clientAuthorization) {
            return this.isVerified;
        }

    }

}
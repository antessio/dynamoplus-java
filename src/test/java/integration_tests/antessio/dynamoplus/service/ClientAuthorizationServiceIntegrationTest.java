package integration_tests.antessio.dynamoplus.service;

import antessio.dynamoplus.persistence.DynamoDb;
import antessio.dynamoplus.persistence.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.system.ClientAuthorizationService;
import antessio.dynamoplus.service.system.bean.client_authorization.*;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.assertj.core.internal.FieldByFieldComparator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class ClientAuthorizationServiceIntegrationTest extends IntegrationTest {
    private ClientAuthorizationService clientAuthorizationService;


    @BeforeEach
    public void setUp() {
        AmazonDynamoDB client = dynamoLocal();
        DynamoDb dynamoDb = new DynamoDb(client);
        DynamoDbTableRepository tableRepository = new DynamoDbTableRepository(dynamoDb, "system");
        clientAuthorizationService = new ClientAuthorizationService(tableRepository);
    }

    @Test
    void testInsertAndLoadIndex() {
        //given
        ClientAuthorizationInterface toInsert = ClientAuthorizationApiKeyBuilder.aClientAuthorizationApiKey()
                .withApiKey("test-1")
                .withClientId("test")
                .withClientScopes(Arrays.asList(
                        ClientAuthorizationScopeBuilder.aClientAuthorizationScope()
                                .withCollectionName("example")
                                .withScopeType(ClientAuthorizationScope.ScopeType.CREATE)
                                .build(),
                        ClientAuthorizationScopeBuilder.aClientAuthorizationScope()
                                .withCollectionName("example")
                                .withScopeType(ClientAuthorizationScope.ScopeType.DELETE)
                                .build()
                ))
                .withWhiteListHosts(Collections.singletonList(
                        "http://localhost:8080"
                ))
                .build();
        //when
        ClientAuthorizationInterface created = clientAuthorizationService.create(toInsert);
        //then
        assertThat(created)
                .isNotNull();
        Optional<ClientAuthorizationInterface> loaded = clientAuthorizationService.getByClientId(created.getClientId());
        assertThat(loaded)
                .get()
                .usingComparator(new FieldByFieldComparator())
                .isEqualToIgnoringGivenFields(created);

    }
}

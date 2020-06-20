package antessio.dynamoplus.service.system;

import antessio.dynamoplus.BaseUnitTest;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.RecordBuilder;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.system.bean.client_authorization.*;
import antessio.dynamoplus.utils.MapUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class ClientAuthorizationServiceTest extends BaseUnitTest {

    private static final String[] IGNORED_FIELDS_API_KEY = {"apiKey", "whiteListHosts"};
    private ClientAuthorizationService clientAuthorizationService;
    private DynamoDbTableRepository tableRepository = mock(DynamoDbTableRepository.class);

    @BeforeEach
    public void setUp() {
        super.setUp();
        clientAuthorizationService = new ClientAuthorizationService(tableRepository);
    }

    @Test
    void testFromDocumentToType() {
        //given
        Document document = new Document(
                MapUtil.ofEntries(
                        MapUtil.entry("type", "api_key")
                )
        );
        //when
        Optional<ClientAuthorizationInterface.ClientAuthorizationType> maybeType = ClientAuthorizationService.fromDocument(document);
        //then
        assertThat(maybeType)
                .get()
                .isEqualToIgnoringGivenFields(ClientAuthorizationInterface.ClientAuthorizationType.API_KEY);
    }

    @Test
    void testGetClientAuthorizationByName() {
        //given
        ClientAuthorizationInterface clientAuthorization = randomClientAuthorization();
        Document document = ClientAuthorizationService.fromClientAuthorizationToMap(clientAuthorization);
        when(tableRepository.get(any(), any())).thenReturn(Optional.of(generator.nextObject(RecordBuilder.class)
                .withDocument(document)
                .build()));
        //when
        Optional<ClientAuthorizationInterface> result = clientAuthorizationService.getByClientId(clientAuthorization.getClientId());
        //then
        ClientAuthorizationApiKey clientAuthorizationApiKey = (ClientAuthorizationApiKey) clientAuthorization;
        assertThat(result).get()
                .isEqualToIgnoringGivenFields(clientAuthorization, IGNORED_FIELDS_API_KEY)
                .isInstanceOf(ClientAuthorizationApiKey.class)
                .extracting(c -> (ClientAuthorizationApiKey) c)
                .matches(a -> a.getApiKey().equals(clientAuthorizationApiKey.getApiKey()) && a.getWhiteListHosts().equals(((ClientAuthorizationApiKey) clientAuthorization).getWhiteListHosts()));

        verify(tableRepository).get(eq("client_authorization#" + clientAuthorization.getClientId()), eq("client_authorization"));
    }

    @Test
    void testInsertNewClientAuthorization() {
        //given
        ClientAuthorizationInterface clientAuthorization = randomClientAuthorization();
        Document document = ClientAuthorizationService.fromClientAuthorizationToMap(clientAuthorization);
        Record expectedRecord = RecordBuilder.aRecord()
                .withPk("client_authorization#" + clientAuthorization.getClientId())
                .withSk("client_authorization")
                .withData(clientAuthorization.getClientId())
                .withDocument(document)
                .build();
        when(tableRepository.create(any())).thenAnswer(returnsFirstArg());
        //when
        ClientAuthorizationInterface result = clientAuthorizationService.create(clientAuthorization);
        //then

        ClientAuthorizationApiKey clientAuthorizationApiKey = (ClientAuthorizationApiKey) clientAuthorization;
        assertThat(result)
                .isEqualToIgnoringGivenFields(clientAuthorization, IGNORED_FIELDS_API_KEY)
                .isInstanceOf(ClientAuthorizationApiKey.class)
                .extracting(c -> (ClientAuthorizationApiKey) c)
                .matches(a -> a.getApiKey().equals(clientAuthorizationApiKey.getApiKey()) && a.getWhiteListHosts().equals(((ClientAuthorizationApiKey) clientAuthorization).getWhiteListHosts()));

        verify(tableRepository).create(refEq(expectedRecord));
    }

    @Test
    void testUpdateClientAuthorization() {
        //given
        ClientAuthorizationInterface clientAuthorization = randomClientAuthorization();
        Document document = ClientAuthorizationService.fromClientAuthorizationToMap(clientAuthorization);
        Record expectedRecord = RecordBuilder.aRecord()
                .withPk("client_authorization#" + clientAuthorization.getClientId())
                .withSk("client_authorization")
                .withData(clientAuthorization.getClientId())
                .withDocument(document)
                .build();
        when(tableRepository.update(any())).thenAnswer(returnsFirstArg());
        //when
        ClientAuthorizationInterface result = clientAuthorizationService.update(clientAuthorization);
        //then
        ClientAuthorizationApiKey clientAuthorizationApiKey = (ClientAuthorizationApiKey) clientAuthorization;
        assertThat(result)
                .isEqualToIgnoringGivenFields(clientAuthorization, IGNORED_FIELDS_API_KEY)
                .isInstanceOf(ClientAuthorizationApiKey.class)
                .extracting(c -> (ClientAuthorizationApiKey) c)
                .matches(a -> a.getApiKey().equals(clientAuthorizationApiKey.getApiKey()) && a.getWhiteListHosts().equals(((ClientAuthorizationApiKey) clientAuthorization).getWhiteListHosts()));

        verify(tableRepository).update(refEq(expectedRecord));
    }

    @Test
    void testDeleteClientAuthorizationByName() {
        //given
        ClientAuthorizationInterface clientAuthorization = randomClientAuthorization();
        Document document = ClientAuthorizationService.fromClientAuthorizationToMap(clientAuthorization);
        doNothing().when(tableRepository).delete(any(), any());
        clientAuthorizationService.delete(clientAuthorization.getClientId());
        //then
        verify(tableRepository).delete(eq("client_authorization#" + clientAuthorization.getClientId()), eq("client_authorization"));
    }


}
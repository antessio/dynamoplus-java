package antessio.dynamoplus.service.system;

import antessio.dynamoplus.dynamodb.RecordFactory;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationBuilder;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationInterface;
import antessio.dynamoplus.service.system.bean.collection.AttributeBuilder;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.collection.CollectionAttributeType;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.utils.ConversionUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public class ClientAuthorizationService {


    private static final Collection CLIENT_AUTHORIZATION_METADATA = new CollectionBuilder()
            .idKey("client_id")
            .name("client_authorization")
            .autoGenerateId(false)
            .attributes(Arrays.asList(
                    new AttributeBuilder()
                            .attributeName("client_id")
                            .attributeType(CollectionAttributeType.STRING)
                            .build(),
                    new AttributeBuilder()
                            .attributeName("client_scopes")
                            .attributeType(CollectionAttributeType.ARRAY)
                            .attributes(Arrays.asList(
                                    new AttributeBuilder()
                                            .attributeName("scope_type")
                                            .attributeType(CollectionAttributeType.STRING)
                                            .build(),
                                    new AttributeBuilder()
                                            .attributeName("collection_name")
                                            .attributeType(CollectionAttributeType.STRING)
                                            .build()
                            ))
                            .build()
            ))
            .createCollection();

    private final DynamoDbTableRepository tableRepository;

    public ClientAuthorizationService(DynamoDbTableRepository tableRepository) {
        this.tableRepository = tableRepository;
    }


    public static Map<String, Object> fromClientAuthorizationToMap(ClientAuthorizationInterface clientAuthorizationInterface) {
        return ConversionUtils.getInstance().convertObject(clientAuthorizationInterface);
    }


    public static ClientAuthorizationInterface fromMapToClientAuthorization(Map<String, Object> document) {
        return ConversionUtils.getInstance().convertMap(document, ClientAuthorizationInterface.class);
    }

    public Optional<ClientAuthorizationInterface> getByClientId(String clientId) {
        ClientAuthorizationInterface clientAuthorization = ClientAuthorizationBuilder.aClientAuthorization()
                .withClientId(clientId)
                .build();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromClientAuthorizationToMap(clientAuthorization), CLIENT_AUTHORIZATION_METADATA);
        return tableRepository.get(record.getPk(), record.getSk())
                .map(Record::getDocument)
                .map(ClientAuthorizationService::fromMapToClientAuthorization);
    }

    public ClientAuthorizationInterface create(ClientAuthorizationInterface clientAuthorizationInterface) {
        Map<String, Object> collectionDocument = fromClientAuthorizationToMap(clientAuthorizationInterface);
        Record record = RecordFactory.getInstance().masterRecordFromDocument(collectionDocument, CLIENT_AUTHORIZATION_METADATA);
        Record recordCreated = tableRepository.create(record);
        return fromMapToClientAuthorization(recordCreated.getDocument());
    }

    public ClientAuthorizationInterface update(ClientAuthorizationInterface clientAuthorizationInterface) {
        Map<String, Object> collectionDocument = fromClientAuthorizationToMap(clientAuthorizationInterface);
        Record record = RecordFactory.getInstance().masterRecordFromDocument(collectionDocument, CLIENT_AUTHORIZATION_METADATA);
        Record recordCreated = tableRepository.update(record);
        return fromMapToClientAuthorization(recordCreated.getDocument());
    }


    public void delete(String clientId) {
        ClientAuthorizationInterface clientAuthorizationInterface = ClientAuthorizationBuilder.aClientAuthorization()
                .withClientId(clientId)
                .build();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromClientAuthorizationToMap(clientAuthorizationInterface), CLIENT_AUTHORIZATION_METADATA);
        tableRepository.delete(record.getPk(), record.getSk());
    }


}

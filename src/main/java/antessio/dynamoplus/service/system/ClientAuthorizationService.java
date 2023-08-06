package antessio.dynamoplus.service.system;

import antessio.dynamoplus.persistence.RecordFactory;
import antessio.dynamoplus.persistence.bean.Record;
import antessio.dynamoplus.persistence.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationBuilder;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationInterface;
import antessio.dynamoplus.service.system.bean.collection.*;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.utils.ConversionUtils;

import java.util.*;
import java.util.stream.Stream;

public class ClientAuthorizationService {

    private static final String CLIENT_AUTHORIZATION_KEY = "client_id";
    private static final List<Attribute> CLIENT_AUTHORIZATION_MANDATORY_ATTRIBUTES = Arrays.asList(
            new AttributeBuilder()
                    .attributeName(CLIENT_AUTHORIZATION_KEY)
                    .attributeType(CollectionAttributeType.STRING)
                    .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                    .build(),
            new AttributeBuilder()
                    .attributeName("type")
                    .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                    .attributeType(CollectionAttributeType.STRING)
                    .build(),
            new AttributeBuilder()
                    .attributeName("client_scopes")
                    .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                    .attributeType(CollectionAttributeType.ARRAY)
                    .attributes(Arrays.asList(
                            new AttributeBuilder()
                                    .attributeName("scope_type")
                                    .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                                    .attributeType(CollectionAttributeType.STRING)
                                    .build(),
                            new AttributeBuilder()
                                    .attributeName("collection_name")
                                    .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                                    .attributeType(CollectionAttributeType.STRING)
                                    .build()
                    ))
                    .build()
    );
    public static final Collection CLIENT_AUTHORIZATION_API_KEY = createClientAuthorizationCollection(
            addAll(
                    CLIENT_AUTHORIZATION_MANDATORY_ATTRIBUTES,
                    Arrays.asList(
                            new AttributeBuilder()
                                    .attributeName("api_key")
                                    .attributeType(CollectionAttributeType.STRING)
                                    .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                                    .build(),
                            new AttributeBuilder()
                                    .attributeName("whiteListHosts")
                                    .attributeType(CollectionAttributeType.ARRAY)
                                    .build()
                    )
            )
    );
    public static final Collection CLIENT_AUTHORIZATION_HTTP_SIGNATURE = createClientAuthorizationCollection(
            addAll(
                    CLIENT_AUTHORIZATION_MANDATORY_ATTRIBUTES,
                    Collections.singletonList(
                            new AttributeBuilder()
                                    .attributeName("client_public_key")
                                    .attributeType(CollectionAttributeType.STRING)
                                    .constraints(Collections.singletonList(CollectionAttributeConstraint.NOT_NULL))
                                    .build()
                    )
            )
    );

    private static <T> List<T> addAll(List<T>... clientAuthorizationMandatoryAttributes) {
        List<T> result = new ArrayList<>();
        for (List<T> l : clientAuthorizationMandatoryAttributes) {
            result.addAll(l);
        }
        return result;
    }

    public static final Collection CLIENT_AUTHORIZATION_METADATA = createClientAuthorizationCollection(CLIENT_AUTHORIZATION_MANDATORY_ATTRIBUTES);

    private static Collection createClientAuthorizationCollection(
            List<Attribute> clientAuthorizationMandatoryAttributes) {
        return CollectionBuilder.aCollection()
                .withIdKey(CLIENT_AUTHORIZATION_KEY)
                .withName("client_authorization")
                .withAutoGenerateId(false)
                .withAttributes(clientAuthorizationMandatoryAttributes)
                .build();
    }


    private final DynamoDbTableRepository tableRepository;

    public ClientAuthorizationService(DynamoDbTableRepository tableRepository) {
        this.tableRepository = tableRepository;
    }

    public static Optional<ClientAuthorizationInterface.ClientAuthorizationType> fromDocument(Document document) {
        return Optional.of(document.get("type"))
                .filter(o -> o instanceof String)
                .map(o -> (String) o)
                .map(String::toUpperCase)
                .filter(s -> Stream.of(ClientAuthorizationInterface.ClientAuthorizationType.values()).anyMatch(t -> t.name().equalsIgnoreCase(s)))
                .map(ClientAuthorizationInterface.ClientAuthorizationType::valueOf);
    }

    public static Document fromClientAuthorizationToMap(ClientAuthorizationInterface clientAuthorizationInterface) {
        return ConversionUtils.getInstance().convertObject(clientAuthorizationInterface);
    }


    public static ClientAuthorizationInterface fromMapToClientAuthorization(Document document) {
        return ConversionUtils.getInstance().convertDocument(document, ClientAuthorizationInterface.class);
    }

    public Optional<ClientAuthorizationInterface> getByClientId(String clientId) {
        ClientAuthorizationInterface clientAuthorization = ClientAuthorizationBuilder.aClientAuthorization()
                .withClientId(clientId)
                .build();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromClientAuthorizationToMap(clientAuthorization), CLIENT_AUTHORIZATION_METADATA);
        return tableRepository.get(record.getRecordKey().getPk(), record.getRecordKey().getSk())
                .map(Record::getDocument)
                .map(ClientAuthorizationService::fromMapToClientAuthorization);
    }

    public ClientAuthorizationInterface create(ClientAuthorizationInterface clientAuthorizationInterface) {
        Document collectionDocument = fromClientAuthorizationToMap(clientAuthorizationInterface);
        Record record = RecordFactory.getInstance().masterRecordFromDocument(collectionDocument, CLIENT_AUTHORIZATION_METADATA);
        Record recordCreated = tableRepository.create(record);
        return fromMapToClientAuthorization(recordCreated.getDocument());
    }

    public ClientAuthorizationInterface update(ClientAuthorizationInterface clientAuthorizationInterface) {
        Document collectionDocument = fromClientAuthorizationToMap(clientAuthorizationInterface);
        Record record = RecordFactory.getInstance().masterRecordFromDocument(collectionDocument, CLIENT_AUTHORIZATION_METADATA);
        Record recordCreated = tableRepository.update(record);
        return fromMapToClientAuthorization(recordCreated.getDocument());
    }


    public void delete(String clientId) {
        ClientAuthorizationInterface clientAuthorizationInterface = ClientAuthorizationBuilder.aClientAuthorization()
                .withClientId(clientId)
                .build();
        Record record = RecordFactory.getInstance().masterRecordFromDocument(fromClientAuthorizationToMap(clientAuthorizationInterface), CLIENT_AUTHORIZATION_METADATA);
        tableRepository.delete(record.getRecordKey().getPk(), record.getRecordKey().getSk());
    }


}

package antessio.dynamoplus.service;

import antessio.dynamoplus.configuration.DynamoDbConfiguration;
import antessio.dynamoplus.dynamodb.DynamoDb;
import antessio.dynamoplus.common.query.Predicate;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.bean.PaginatedResult;
import antessio.dynamoplus.service.domain.DomainService;
import antessio.dynamoplus.service.exception.CollectionNotFoundException;
import antessio.dynamoplus.service.exception.DocumentNotFoundException;
import antessio.dynamoplus.service.system.ClientAuthorizationService;
import antessio.dynamoplus.service.system.CollectionService;
import antessio.dynamoplus.service.system.IndexService;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationInterface;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.service.system.bean.index.Index;
import antessio.dynamoplus.service.system.bean.index.IndexBuilder;
import antessio.dynamoplus.service.validation.ValidationService;
import antessio.dynamoplus.utils.ConversionUtils;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import java.util.Arrays;
import java.util.UUID;

public class DynamoPlusService {
    private static final String COLLECTION_COLLECTION_NAME = "collection";
    private static final String INDEX_COLLECTION_NAME = "index";
    private static final String CLIENT_AUTHORIZATION_COLLECTION_NAME = "client_authorization";
    private final IndexService indexService;
    private final CollectionService collectionService;
    private final ClientAuthorizationService clientAuthorizationService;
    private final DomainService domainService;
    private final ValidationService validationService;


    DynamoPlusService(IndexService indexService, CollectionService collectionService,
                      ClientAuthorizationService clientAuthorizationService,
                      DomainService domainService,
                      ValidationService validationService) {
        this.indexService = indexService;
        this.clientAuthorizationService = clientAuthorizationService;
        this.collectionService = collectionService;
        this.domainService = domainService;
        this.validationService = validationService;
    }

    public DynamoPlusService(DynamoDbConfiguration dynamoDbConfiguration) {
        if (dynamoDbConfiguration.getAccessKey() == null || dynamoDbConfiguration.getSecret() == null) {
            throw new IllegalArgumentException("Aws credentials not provided");
        }
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(dynamoDbConfiguration.getRegion())
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(dynamoDbConfiguration.getAccessKey(), dynamoDbConfiguration.getSecret())))
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(dynamoDbConfiguration.getEndpoint(), null)
                )
                .build();
        DynamoDb dynamoDb = new DynamoDb(client);
        DynamoDbTableRepository systemRepository = new DynamoDbTableRepository(dynamoDb, "system");
        DynamoDbTableRepository domainRepository = new DynamoDbTableRepository(dynamoDb, "domain");
        this.indexService = new IndexService(systemRepository);
        this.collectionService = new CollectionService(systemRepository);
        this.clientAuthorizationService = new ClientAuthorizationService(systemRepository);
        this.domainService = new DomainService(domainRepository);
        this.validationService = ValidationService.getInstance();
    }

    //================================== create =============================
    public Document createDocument(String collectionName, Document document) {
        //check if collection is system
        //if true switch index or collection => insert index or collection
        //if not system
        //from collection execute validation on json schema
        //=> insert document
        switch (collectionName) {
            case COLLECTION_COLLECTION_NAME:
                return createCollection(document);
            case INDEX_COLLECTION_NAME:
                return createNewIndex(document);
            case CLIENT_AUTHORIZATION_COLLECTION_NAME:
                return createNewClientAuthorization(document);
            default:
                return collectionService.getCollectionByName(collectionName)
                        .map(c -> this.createNewDocument(document, c))
                        .orElseThrow(() -> new CollectionNotFoundException(String.format("collection %s not found", collectionName)));
        }

    }

    private Document createNewDocument(Document document, Collection c) {
        validationService.validate(document, c);
        return domainService.createDocument(document, c);
    }

    private Document createNewClientAuthorization(Document document) {
        validationService.validateClientAuthorization(document);
        ClientAuthorizationInterface c = ConversionUtils.getInstance().convertDocument(document, ClientAuthorizationInterface.class);
        ClientAuthorizationInterface clientAuthorization = clientAuthorizationService.create(c);
        return ConversionUtils.getInstance().convertObject(clientAuthorization);
    }

    private Document createNewIndex(Document document) {
        validationService.validateIndex(document);
        Index index = ConversionUtils.getInstance().convertDocument(document, Index.class);
        Index created = indexService.createIndex(index);
        return ConversionUtils.getInstance().convertObject(created);
    }

    private Document createCollection(Document document) {
        validationService.validateCollection(document);
        Collection c = ConversionUtils.getInstance().convertDocument(document, Collection.class);
        Collection createdCollection = collectionService.createCollection(c);
        return ConversionUtils.getInstance().convertObject(createdCollection);

    }
    //=================================================================


    public Document updateDocument(String collectionName, String id, Document document) {
        //check if collection is system
        //if true switch index or collection => update index or collection
        //if not system
        //from collection execute validation on json schema
        //=> update document
        switch (collectionName) {
            case COLLECTION_COLLECTION_NAME:
                return updateExistingCollection(id, document);
            case INDEX_COLLECTION_NAME:
                return updateExistingIndex(id, document);
            case CLIENT_AUTHORIZATION_COLLECTION_NAME:
                return updateExistingClientAuthorization(id, document);
            default:
                return collectionService.getCollectionByName(collectionName)
                        .map(c -> this.updateExistingDocument(id, document, c))
                        .orElseThrow(() -> new CollectionNotFoundException(String.format("collection %s not found", collectionName)));
        }
    }

    private Document updateExistingClientAuthorization(String id, Document document) {
        return null;
    }

    private Document updateExistingIndex(String id, Document document) {
        validationService.validate(document, IndexService.INDEX_COLLECTION);
        Index index = ConversionUtils.getInstance().convertDocument(document, Index.class);
        Index updatedIndex = indexService.updateIndex(IndexBuilder.anIndex(index)
                .withUid(UUID.fromString(id))
                .build());
        return ConversionUtils.getInstance().convertObject(updatedIndex);
    }

    private Document updateExistingDocument(String id,
                                            Document document,
                                            Collection collection) {
        validationService.validate(document, collection);
        return domainService.getDocument(id, collection)
                .map(existingDocument -> DomainService.mergeDocuments(existingDocument, document, collection.getIdKey()))
                .map(d -> domainService.updateDocument(d, collection))
                .orElseThrow(() -> new DocumentNotFoundException(id, collection));
    }

    private Document updateExistingCollection(String id, Document document) {
        Collection collection = ConversionUtils.getInstance().convertDocument(document, Collection.class);
        Collection updatedCollection = collectionService.updateCollection(CollectionBuilder.aCollection(collection)
                .withName(id)
                .build());
        return ConversionUtils.getInstance().convertObject(updatedCollection);
    }

    public Document getDocument(String collectionName, String id) {
        //check if collection is system
        //if true switch index or collection => get index or collection
        //if not system
        //from collection execute validation on json schema
        //=> get document
        return null;
    }

    public void delete(String collectionName, String id) {
        //check if collection is system
        //if true switch index or collection => delete index or collection
        //if not system
        //from collection execute validation on json schema
        //=> delete document
    }

    public PaginatedResult<Document> query(String collectionName,
                                           Predicate predicate) {
        return null;
    }

    private boolean isSystemCollection(Collection c) {
        return Arrays.asList(COLLECTION_COLLECTION_NAME, INDEX_COLLECTION_NAME, CLIENT_AUTHORIZATION_COLLECTION_NAME).contains(c.getName());
    }


}

package antessio.dynamoplus.service;

import antessio.dynamoplus.DynamoPlus;
import antessio.dynamoplus.common.query.And;
import antessio.dynamoplus.common.query.Any;
import antessio.dynamoplus.common.query.Eq;
import antessio.dynamoplus.common.query.Query;
import antessio.dynamoplus.common.query.Range;
import antessio.dynamoplus.configuration.DynamoDbConfiguration;
import antessio.dynamoplus.persistence.DynamoDb;
import antessio.dynamoplus.common.query.Predicate;
import antessio.dynamoplus.persistence.impl.DynamoDbTableRepository;
import antessio.dynamoplus.security.Authorization;
import antessio.dynamoplus.security.SecurityException;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.bean.PaginatedResult;
import antessio.dynamoplus.service.bean.QueryBuilder;
import antessio.dynamoplus.service.domain.DomainService;
import antessio.dynamoplus.service.exception.CollectionNotFoundException;
import antessio.dynamoplus.service.exception.DocumentNotFoundException;
import antessio.dynamoplus.service.system.ClientAuthorizationService;
import antessio.dynamoplus.service.system.CollectionService;
import antessio.dynamoplus.service.system.IndexService;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorization;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationBuilder;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class DynamoPlusService implements DynamoPlus {

    private static final String COLLECTION_COLLECTION_NAME = "collection";
    private static final String INDEX_COLLECTION_NAME = "index";
    private static final String CLIENT_AUTHORIZATION_COLLECTION_NAME = "client_authorization";
    private final IndexService indexService;
    private final CollectionService collectionService;
    private final ClientAuthorizationService clientAuthorizationService;
    private final DomainService domainService;
    private final ValidationService validationService;

    private final boolean isAuthorizationCheckEnabled;


    DynamoPlusService(
            IndexService indexService, CollectionService collectionService,
            ClientAuthorizationService clientAuthorizationService,
            DomainService domainService,
            ValidationService validationService,
            boolean isAuthorizationCheckEnabled) {
        this.indexService = indexService;
        this.clientAuthorizationService = clientAuthorizationService;
        this.collectionService = collectionService;
        this.domainService = domainService;
        this.validationService = validationService;
        this.isAuthorizationCheckEnabled = isAuthorizationCheckEnabled;
    }

    public DynamoPlusService(DynamoDbConfiguration dynamoDbConfiguration) {
        if (dynamoDbConfiguration.getAccessKey() == null || dynamoDbConfiguration.getSecret() == null) {
            throw new IllegalArgumentException("Aws credentials not provided");
        }
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                                                           .withRegion(dynamoDbConfiguration.getRegion())
                                                           .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                                                                   dynamoDbConfiguration.getAccessKey(),
                                                                   dynamoDbConfiguration.getSecret())))
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
        this.isAuthorizationCheckEnabled = true;
    }

    //================================== create =============================
    @Override
    public Document createDocument(String collectionName, Document document, Authorization authorization) {
        //check if collection is system
        //if true switch index or collection => insert index or collection
        //if not system
        //from collection execute validation on json schema
        //=> insert document
        verifyAuthorization(authorization);
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

    private void verifyAuthorization(Authorization authorization) {
        if (isAuthorizationCheckEnabled) {
            clientAuthorizationService.getByClientId(authorization.getClientId())
                                      .map(authorization::verify)
                                      .filter(Boolean::booleanValue)
                                      .orElseThrow(() -> new SecurityException("wrong security credentials"));
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
        return convertClientAuthorizationToDocument(clientAuthorization);
    }

    private static Document convertClientAuthorizationToDocument(ClientAuthorizationInterface clientAuthorization) {
        return ConversionUtils.getInstance().convertObject(
                clientAuthorization,
                ClientAuthorizationService.CLIENT_AUTHORIZATION_METADATA.getIdKey());
    }

    private Document createNewIndex(Document document) {
        validationService.validateIndex(document);
        Index index = ConversionUtils.getInstance().convertDocument(document, Index.class);
        Index created = indexService.createIndex(index);
        return convertIndexToDocument(created);
    }

    private static Document convertIndexToDocument(Index created) {
        return ConversionUtils.getInstance().convertObject(created, IndexService.INDEX_COLLECTION.getIdKey());
    }

    private Document createCollection(Document document) {
        validationService.validateCollection(document);
        Collection c = ConversionUtils.getInstance().convertDocument(document, Collection.class);
        Collection createdCollection = collectionService.createCollection(c);
        return ConversionUtils.getInstance().convertObject(createdCollection);

    }
    //=================================================================


    @Override
    public Document updateDocument(String collectionName, String id, Document document, Authorization authorization) {
        //check if collection is system
        //if true switch index or collection => update index or collection
        //if not system
        //from collection execute validation on json schema
        //=> update document
        verifyAuthorization(authorization);
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
        validationService.validate(document, ClientAuthorizationService.CLIENT_AUTHORIZATION_METADATA);
        ClientAuthorization clientAuthorization = ConversionUtils.getInstance().convertDocument(document, ClientAuthorization.class);
        clientAuthorizationService.update(ClientAuthorizationBuilder.aClientAuthorization().withClientId(id).build());
        return convertClientAuthorizationToDocument(clientAuthorization);
    }

    private Document updateExistingIndex(String id, Document document) {
        validationService.validate(document, IndexService.INDEX_COLLECTION);
        Index index = ConversionUtils.getInstance().convertDocument(document, Index.class);
        Index updatedIndex = indexService.updateIndex(IndexBuilder.anIndex(index)
                                                                  .withUid(UUID.fromString(id))
                                                                  .build());
        return convertIndexToDocument(updatedIndex);
    }

    private Document updateExistingDocument(
            String id,
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

    @Override
    public Optional<Document> getDocument(String collectionName, String id, Authorization authorization) {
        //check if collection is system
        //if true switch index or collection => get index or collection
        //if not system
        //from collection execute validation on json schema
        //=> get document
        verifyAuthorization(authorization);
        switch (collectionName) {
            case COLLECTION_COLLECTION_NAME:
                return collectionService.getCollectionByName(id)
                                        .map(c -> ConversionUtils.getInstance().convertObject(c));
            case INDEX_COLLECTION_NAME:
                UUID indexId = UUID.fromString(id);
                return indexService.getById(indexId)
                                   .map(i -> convertIndexToDocument(i));
            case CLIENT_AUTHORIZATION_COLLECTION_NAME:
                return clientAuthorizationService.getByClientId(id)
                                                 .map(c -> convertClientAuthorizationToDocument(c));
            default:
                return collectionService.getCollectionByName(collectionName)
                                        .map(c -> domainService.getDocument(id, c))
                                        .orElseThrow(() -> new CollectionNotFoundException(String.format("collection %s not found", collectionName)));
        }
    }

    @Override
    public void delete(String collectionName, String id, Authorization authorization) {
        //check if collection is system
        //if true switch index or collection => delete index or collection
        //if not system
        //from collection execute validation on json schema
        //=> delete document
        verifyAuthorization(authorization);
        switch (collectionName) {
            case COLLECTION_COLLECTION_NAME:
                collectionService.delete(id);
                break;
            case INDEX_COLLECTION_NAME:
                UUID indexId = UUID.fromString(id);
                indexService.deleteIndexById(indexId);
                break;
            case CLIENT_AUTHORIZATION_COLLECTION_NAME:
                clientAuthorizationService.delete(id);
                break;
            default: {
                Collection collection = collectionService.getCollectionByName(collectionName)
                                                         .orElseThrow(() -> new CollectionNotFoundException(String.format(
                                                                 "collection %s not found",
                                                                 collectionName)));
                domainService.deleteDocument(id, collection);

            }
        }
    }


    @Override
    public PaginatedResult<Document, String> query(Query query, Authorization authorization) {
        verifyAuthorization(authorization);
        String collectionName = query.getCollectionName();
        Predicate predicate = query.getPredicate();
        Optional<UUID> maybeCursor = Optional.ofNullable(query.getLastId())
                                             .map(UUID::fromString);
        int limit = Optional.ofNullable(query.getLimit())
                            .orElse(20);
        switch (collectionName) {
            case COLLECTION_COLLECTION_NAME, CLIENT_AUTHORIZATION_COLLECTION_NAME:
                throw new IllegalArgumentException("no access pattern matching query");
            case INDEX_COLLECTION_NAME:
                if (predicate instanceof Eq eq) {
                    if (eq.getFieldName().equals("collection_name")) {
                        List<Document> results = indexService.getByCollectionName(eq.getFieldValue(), maybeCursor.orElse(null))
                                                             .map(index -> convertIndexToDocument(index))
                                                             .limit(limit + 1)
                                                             .toList();
                        String nextId = results.size() > limit ?
                                        results.get(results.size() - 1)
                                               .toString() : null;
                        return new PaginatedResult<>(
                                results.stream().limit(limit).collect(Collectors.toList()),
                                nextId != null,
                                nextId);
                    }
                } else if (predicate instanceof And and) {
                    if (and.getAnd().size() == 2) {
                        Predicate condition1 = and.getAnd().get(0);
                        Predicate condition2 = and.getAnd().get(1);
                        if (condition1 instanceof Eq eq1 && condition2 instanceof Eq eq2) {
                            if (eq1.getFieldName().equals("collection_name") && eq2.getFieldName().equals("name")) {
                                return indexService.getByCollectionNameAndName(eq1.getFieldValue(), eq2.getFieldValue())
                                                   .map(index -> convertIndexToDocument(index))
                                                   .map(document -> new PaginatedResult<Document, String>(List.of(document), false))
                                                   .orElseGet(() -> new PaginatedResult<>(Collections.emptyList(), false));
                            }
                        }
                    }
                }
                throw new IllegalArgumentException("no access pattern matching query");
            default: {
                Collection collection = collectionService.getCollectionByName(collectionName)
                                                         .orElseThrow(() -> new CollectionNotFoundException(String.format(
                                                                 "collection %s not found",
                                                                 collectionName)));
                return indexService.getByCollectionName(collectionName, null)
                                   .map(i -> this.indexMatchingQuery(i, query))
                                   .filter(Optional::isPresent)
                                   .map(Optional::get)
                                   .findFirst()
                                   .map(q -> domainService.query(q, collection))
                                   .orElseThrow(() -> new IllegalArgumentException("no index matching the access pattern"));

            }
        }
    }

    private Optional<antessio.dynamoplus.service.bean.Query> indexMatchingQuery(Index i, Query query) {
        List<String> conditions = extractConditionsFromPredicate(query.getPredicate());
        if (i.getConditions().equals(conditions)) {
            return Optional.of(new QueryBuilder()
                                       .indexName(i.getName())
                                       .lastKey(query.getLastId())
                                       .limit(query.getLimit())
                                       .predicate(query.getPredicate())
                                       .build());
        }
        return Optional.empty();
    }

    private List<String> extractConditionsFromPredicate(Predicate predicate) {
        List<String> conditions = new ArrayList<>();
        if (predicate instanceof Eq eq) {
            conditions.add(eq.getFieldName());
        } else if (predicate instanceof And and) {
            conditions.addAll(and.getAnd()
                                 .stream()
                                 .map(this::extractConditionsFromPredicate)
                                 .flatMap(java.util.Collection::stream)
                                 .toList());
        } else if (predicate instanceof Range range) {
            conditions.add(range.getFieldName());
        }
        return conditions;
    }

    private boolean isSystemCollection(Collection c) {
        return Arrays.asList(COLLECTION_COLLECTION_NAME, INDEX_COLLECTION_NAME, CLIENT_AUTHORIZATION_COLLECTION_NAME).contains(c.getName());
    }


}

package antessio.dynamoplus.service;

import antessio.dynamoplus.configuration.DynamoDbConfiguration;
import antessio.dynamoplus.dynamodb.DynamoDb;
import antessio.dynamoplus.dynamodb.bean.query.Predicate;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.bean.PaginatedResult;
import antessio.dynamoplus.service.domain.DomainService;
import antessio.dynamoplus.service.system.ClientAuthorizationService;
import antessio.dynamoplus.service.system.CollectionService;
import antessio.dynamoplus.service.system.IndexService;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import java.util.Map;

public class DynamoPlusService {
    private final IndexService indexService;
    private final CollectionService collectionService;
    private final ClientAuthorizationService clientAuthorizationService;
    private final DomainService domainService;

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
        this.indexService = new IndexService(systemRepository);
        this.collectionService = new CollectionService(systemRepository);
        this.clientAuthorizationService = new ClientAuthorizationService(systemRepository);
        DynamoDbTableRepository domainRepository = new DynamoDbTableRepository(dynamoDb, "domain");
        this.domainService = new DomainService(domainRepository);
    }

    public Map<String, Object> createDocument(String collectionName, Map<String, Object> document) {
        //check if collection is system
        //if true switch index or collection => insert index or collection
        //if not system
        //from collection execute validation on json schema
        //=> insert document
        return null;
    }

    public Map<String, Object> updateDocument(String collectionName, String id, Map<String, Object> document) {
        //check if collection is system
        //if true switch index or collection => update index or collection
        //if not system
        //from collection execute validation on json schema
        //=> update document
        return null;
    }

    public Map<String, Object> getDocument(String collectionName, String id) {
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

    public PaginatedResult<Map<String, Object>> query(String collectionName,
                                                      Predicate predicate) {
        return null;
    }

}

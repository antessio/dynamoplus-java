package antessio.dynamoplus.service.domain;

import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;

public class DomainService {
    private final DynamoDbTableRepository repository;

    public DomainService(DynamoDbTableRepository repository) {
        this.repository = repository;
    }


}

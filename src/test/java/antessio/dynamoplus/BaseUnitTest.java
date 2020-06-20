package antessio.dynamoplus;

import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.system.ClientAuthorizationService;
import antessio.dynamoplus.service.system.IndexService;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationApiKeyBuilder;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationInterface;
import antessio.dynamoplus.service.system.bean.collection.AttributeBuilder;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.service.system.bean.index.Index;
import antessio.dynamoplus.service.system.bean.index.IndexBuilder;
import org.jeasy.random.EasyRandom;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentCaptor;

import static java.util.stream.Collectors.toList;

public class BaseUnitTest {
    protected EasyRandom generator;

    @BeforeEach
    protected void setUp() {
        generator = new EasyRandom();
    }


    protected <T> ArgumentCaptor<T> capture(Class<T> cls) {
        return ArgumentCaptor.forClass(cls);
    }

    protected Document randomIndexDocument() {
        Index index = randomIndex();
        return IndexService.fromIndexToMap(index);
    }

    protected Index randomIndex() {
        return generator.nextObject(IndexBuilder.class)
                .withCollection(generator.nextObject(CollectionBuilder.class)
                        .withAttributes(generator.objects(AttributeBuilder.class, 3)
                                .map(b -> b.attributes(null).build())
                                .collect(toList()))
                        .build())
                .withConditions(generator.objects(String.class, 3).collect(toList()))
                .build();
    }

    protected Document randomClientAuthorizationDocument() {
        return ClientAuthorizationService.fromClientAuthorizationToMap(randomClientAuthorization());
    }

    protected ClientAuthorizationInterface randomClientAuthorization() {
        return generator.nextObject(ClientAuthorizationApiKeyBuilder.class)
                .build();
    }
}

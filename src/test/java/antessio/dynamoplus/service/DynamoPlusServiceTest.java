package antessio.dynamoplus.service;

import antessio.dynamoplus.BaseUnitTest;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.domain.DomainService;
import antessio.dynamoplus.service.system.ClientAuthorizationService;
import antessio.dynamoplus.service.system.CollectionService;
import antessio.dynamoplus.service.system.IndexService;
import antessio.dynamoplus.service.system.bean.collection.AttributeBuilder;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.service.validation.ValidationService;
import antessio.dynamoplus.utils.MapUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class DynamoPlusServiceTest extends BaseUnitTest {

    private DynamoPlusService service;
    private IndexService indexService = mock(IndexService.class);
    private CollectionService collectionService = mock(CollectionService.class);
    private ClientAuthorizationService clientAuthorizationService = mock(ClientAuthorizationService.class);
    private DomainService domainService = mock(DomainService.class);
    private ValidationService validationService = mock(ValidationService.class);

    @BeforeEach
    protected void setUp() {
        super.setUp();
        service = new DynamoPlusService(
                indexService,
                collectionService,
                clientAuthorizationService,
                domainService,
                validationService);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(indexService, collectionService, clientAuthorizationService, domainService, validationService);
    }

    @Test
    void testCreateDocument() {
        //given
        String expectedCollectionName = "example";
        Document expectedDocument = new Document(
                MapUtil.ofEntries(
                        MapUtil.entry("key", "value")
                )
        );
        Collection expectedCollection = generator.nextObject(CollectionBuilder.class)
                .withAttributes(generator.objects(AttributeBuilder.class, 3).map(a -> a.attributes(null).build()).collect(Collectors.toList()))
                .build();
        when(collectionService.getCollectionByName(eq(expectedCollectionName))).thenReturn(Optional.of(
                expectedCollection
        ));
        doNothing().when(validationService).validate(any(), any());
        when(domainService.createDocument(any(), any())).thenAnswer(returnsFirstArg());
        //when
        Document document = service.createDocument(expectedCollectionName, expectedDocument);
        //then
        assertThat(document).isEqualTo(expectedDocument);
        verify(collectionService).getCollectionByName(eq(expectedCollectionName));
        verify(validationService).validate(eq(expectedDocument), refEq(expectedCollection));
        verify(domainService).createDocument(eq(expectedDocument), refEq(expectedCollection));

    }

    @Test
    void testCreateCollection() {
        //given
        String expectedCollectionName = "collection";
        Document expectedDocument = new Document(
                MapUtil.ofEntries(
                        MapUtil.entry("id_key", "id"),
                        MapUtil.entry("name", "example")
                )
        );
        doNothing().when(validationService).validateCollection(any());
        when(collectionService.createCollection(any())).thenAnswer(returnsFirstArg());
        //when
        Document document = service.createDocument(expectedCollectionName, expectedDocument);
        //then
        assertThat(document).isEqualTo(expectedDocument);
        verify(validationService).validateCollection(refEq(expectedDocument));
        verify(collectionService).createCollection(any());
    }

    @Test
    void testCreateIndex() {
        //given
        String expectedCollectionName = "index";
        Document expectedDocument = randomIndexDocument();
        doNothing().when(validationService).validateIndex(any());
        when(indexService.createIndex(any())).thenAnswer(returnsFirstArg());
        //when
        Document document = service.createDocument(expectedCollectionName, expectedDocument);
        //then
        assertThat(document).isEqualTo(expectedDocument);
        verify(validationService).validateIndex(refEq(expectedDocument));
        verify(indexService).createIndex(any());
    }

    @Test
    void testClientAuthorization() {
        //given
        String expectedCollectionName = "client_authorization";
        Document expectedDocument = randomClientAuthorizationDocument();
        doNothing().when(validationService).validateClientAuthorization(any());
        when(clientAuthorizationService.create(any())).thenAnswer(returnsFirstArg());
        //when
        Document document = service.createDocument(expectedCollectionName, expectedDocument);
        //then
        assertThat(document).isEqualTo(expectedDocument);
        verify(validationService).validateClientAuthorization(refEq(expectedDocument));
        verify(clientAuthorizationService).create(any());
    }

    @Test
    void updateDocument() {
        //given
        String expectedCollectionName = "example";
        String expectedId = UUID.randomUUID().toString();
        Document expectedDocument = new Document(
                MapUtil.ofEntries(
                        MapUtil.entry("id", expectedId),
                        MapUtil.entry("key", "value")
                )
        );
        Collection expectedCollection = generator.nextObject(CollectionBuilder.class)
                .withAttributes(generator.objects(AttributeBuilder.class, 3).map(a -> a.attributes(null).build()).collect(Collectors.toList()))
                .build();
        when(collectionService.getCollectionByName(eq(expectedCollectionName))).thenReturn(Optional.of(
                expectedCollection
        ));
        when(domainService.getDocument(eq(expectedId), refEq(expectedCollection))).thenReturn(Optional.of(expectedDocument));
        doNothing().when(validationService).validate(any(), any());
        when(domainService.updateDocument(any(), any())).thenAnswer(returnsFirstArg());
        //when
        Document document = service.updateDocument(expectedCollectionName, expectedId, expectedDocument);
        //then
        assertThat(document).isEqualTo(expectedDocument);
        verify(collectionService).getCollectionByName(eq(expectedCollectionName));
        verify(validationService).validate(eq(expectedDocument), refEq(expectedCollection));
        verify(domainService).getDocument(eq(expectedId), refEq(expectedCollection));
        verify(domainService).updateDocument(eq(expectedDocument), refEq(expectedCollection));
    }

    @Test
    void getDocument() {
        //given
        //when
        //then
    }

    @Test
    void delete() {
        //given
        //when
        //then
    }

    @Test
    void query() {
        //given
        //when
        //then
    }
}
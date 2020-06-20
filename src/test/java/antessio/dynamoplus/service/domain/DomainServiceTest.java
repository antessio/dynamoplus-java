package antessio.dynamoplus.service.domain;

import antessio.dynamoplus.BaseUnitTest;
import antessio.dynamoplus.common.query.PredicateBuilder;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.RecordBuilder;
import antessio.dynamoplus.dynamodb.bean.query.QueryResultsWithCursor;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.bean.PaginatedResult;
import antessio.dynamoplus.service.bean.Query;
import antessio.dynamoplus.service.bean.QueryBuilder;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DomainServiceTest extends BaseUnitTest {
    private DomainService domainService;
    private DynamoDbTableRepository repository = mock(DynamoDbTableRepository.class);

    @BeforeEach
    protected void setUp() {
        super.setUp();
        domainService = new DomainService(repository);
    }

    @Test
    void testCreateDocument() {
        //given
        Collection collection = CollectionBuilder.aCollection()
                .withName("example")
                .withIdKey("id")
                .build();
        UUID expectedId = UUID.randomUUID();
        Document document = new Document("id", expectedId.toString());
        when(repository.create(any())).thenAnswer(returnsFirstArg());
        //when
        Document createdDocument = domainService.createDocument(document, collection);
        //then
        assertThat(createdDocument)
                .isEqualTo(document);
        ArgumentCaptor<Record> argumentCaptor = capture(Record.class);
        verify(repository).create(argumentCaptor.capture());
        assertThat(argumentCaptor.getValue())
                .matches(r -> r.getPk().equals("example#" + expectedId))
                .matches(r -> r.getSk().equals("example"))
                .matches(r -> r.getData().equals(expectedId.toString()));

    }

    @Test
    void testUpdateDocument() {
        //given
        Collection collection = CollectionBuilder.aCollection()
                .withName("example")
                .withIdKey("id")
                .build();
        UUID expectedId = UUID.randomUUID();
        Document document = new Document("id", expectedId.toString());
        when(repository.update(any())).thenAnswer(returnsFirstArg());
        //when
        Document createdDocument = domainService.updateDocument(document, collection);
        //then
        assertThat(createdDocument)
                .isEqualTo(document);
        ArgumentCaptor<Record> argumentCaptor = capture(Record.class);
        verify(repository).update(argumentCaptor.capture());
        assertThat(argumentCaptor.getValue())
                .matches(r -> r.getPk().equals("example#" + expectedId))
                .matches(r -> r.getSk().equals("example"))
                .matches(r -> r.getData().equals(expectedId.toString()));
    }

    @Test
    void testGetDocument() {
        //given
        Collection collection = CollectionBuilder.aCollection()
                .withName("example")
                .withIdKey("id")
                .build();
        UUID expectedId = UUID.randomUUID();
        Document document = new Document("id", expectedId.toString());
        when(repository.get(any(), any())).thenReturn(Optional.of(RecordBuilder.aRecord()
                .withPk("example#" + expectedId.toString())
                .withSk("example")
                .withData(expectedId.toString())
                .withDocument(document)
                .build()));
        //when
        Optional<Document> maybeDocument = domainService.getDocument(expectedId.toString(), collection);
        //then
        assertThat(maybeDocument)
                .get()
                .isEqualTo(document);
        verify(repository).get(eq("example#" + expectedId), eq("example"));
    }

    @Test
    void testQuery() {
        //given
        UUID expectedLastId = UUID.randomUUID();
        Collection collection = CollectionBuilder.aCollection()
                .withName("example")
                .withIdKey("id")
                .build();
        Query query = new QueryBuilder()
                .indexName("name#city")
                .lastKey(expectedLastId.toString())
                .limit(3)
                .predicate(new PredicateBuilder().withAnd(
                        Arrays.asList(
                                new PredicateBuilder()
                                        .withEq("name", "x"),
                                new PredicateBuilder()
                                        .withEq("city", "y")

                        )
                ))
                .build();
        when(repository.get(any(), any())).thenReturn(Optional.ofNullable(
                generator.nextObject(RecordBuilder.class)
                        .withPk("example#" + expectedLastId)
                        .withSk("example")
                        .withData(expectedLastId.toString())
                        .withDocument(new Document("id", expectedLastId.toString()))
                        .build()
        ));
        when(repository.query(any())).thenReturn(new QueryResultsWithCursor(
                generator.objects(Record.class, 3)
                        .collect(Collectors.toList()),
                null
        ));
        //when
        PaginatedResult<Document> results = domainService.query(query, collection);
        //then
        assertThat(results)
                .matches(r -> !r.isHasMore())
                .matches(r -> r.getData().size() == 3);
        ArgumentCaptor<antessio.dynamoplus.dynamodb.bean.Query> argumentCapturer = capture(antessio.dynamoplus.dynamodb.bean.Query.class);
        verify(repository).query(argumentCapturer.capture());
        assertThat(argumentCapturer.getValue())
                .matches(q -> q.getPartitionKey().equals("example#name#city"))
                .matches(q -> q.getPredicate().equals(query.getPredicate()))
                .matches(q -> q.getLimit() == 3)
                .matches(r -> r.getStartFrom().getPk().equals("example#" + expectedLastId));
        verify(repository).get(eq("example#" + expectedLastId), eq("example"));
    }

    @Test
    void testDeleteDocument() {
        //given
        Collection collection = CollectionBuilder.aCollection()
                .withName("example")
                .withIdKey("id")
                .build();
        UUID expectedId = UUID.randomUUID();
        doNothing().when(repository).delete(any(), any());
        //when
        domainService.deleteDocument(expectedId.toString(), collection);
        //then
        verify(repository).delete(eq("example#" + expectedId), eq("example"));
    }
}
package antessio.dynamoplus.service.system;

import antessio.dynamoplus.BaseUnitTest;
import antessio.dynamoplus.dynamodb.bean.Query;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.RecordBuilder;
import antessio.dynamoplus.dynamodb.bean.query.QueryResultsWithCursor;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.system.IndexService;
import antessio.dynamoplus.service.system.bean.collection.AttributeBuilder;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.service.system.bean.index.Index;
import antessio.dynamoplus.service.system.bean.index.IndexBuilder;
import org.assertj.core.internal.FieldByFieldComparator;
import org.jeasy.random.EasyRandom;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class IndexServiceTest extends BaseUnitTest {
    private DynamoDbTableRepository repoTable = mock(DynamoDbTableRepository.class);
    private IndexService indexService;

    @BeforeEach
    protected void setUp() {
        super.setUp();
        indexService = new IndexService(repoTable);


    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(repoTable);
    }

    @Test
    void testCreateIndex() {
        //given
        Index index = randomIndex();
        Document document = IndexService.fromIndexToMap(index);
        Record expectedRecord = RecordBuilder.aRecord()
                .withPk("index#" + index.getUid())
                .withSk("index")
                .withData(index.getUid().toString())
                .withDocument(document)
                .build();

        when(repoTable.create(any())).thenReturn(generator.nextObject(RecordBuilder.class)
                .withDocument(document)
                .build());
        //when
        Index indexCreated = indexService.createIndex(index);
        //then
        assertThat(indexCreated).isNotNull();
        verify(repoTable).create(refEq(expectedRecord));
    }


    @Test
    void testCreateGsiRows() {
        //given
        Index index = randomIndex();
        Document document = IndexService.fromIndexToMap(index);
        Record expectedRecord = RecordBuilder.aRecord()
                .withPk("index#" + index.getUid())
                .withSk("index#collection.name#name")
                .withData(String.format("%s#%s", index.getCollection().getName(), index.getName()))
                .withDocument(document)
                .build();

        when(repoTable.create(any())).thenAnswer(returnsFirstArg());
        //when
        indexService.createGsiRows(index);
        //then
        verify(repoTable, times(1)).create(refEq(expectedRecord));
    }


    @Test
    void testGetById() {
        //given
        UUID id = UUID.randomUUID();
        Index index = randomIndex();
        Document document = IndexService.fromIndexToMap(index);
        when(repoTable.get(any(), any())).thenReturn(Optional.of(generator.nextObject(RecordBuilder.class)
                .withDocument(document)
                .build()));
        //when
        Optional<Index> result = indexService.getById(id);
        //then
        assertThat(result)
                .get()
                .usingComparator(new FieldByFieldComparator())
                .isEqualToIgnoringGivenFields(index, "collection");
        verify(repoTable).get(eq("index#" + id), eq("index"));
    }

    @Test
    void testGetByCollectionName() {
        //given
        Index index = randomIndex();
        Document document = IndexService.fromIndexToMap(index);
        when(repoTable.query(any())).thenReturn(
                new QueryResultsWithCursor(generator.objects(RecordBuilder.class, 2)
                        .map(b -> b.withDocument(document)
                                .build()).collect(toList()),
                        null)
        );
        //when
        Optional<Index> result = indexService.getByCollectionName(index.getCollection().getName());
        //then
        assertThat(result)
                .get()
                .usingComparator(new FieldByFieldComparator())
                .isEqualToIgnoringGivenFields(index, "collection");
        ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
        verify(repoTable).query(captor.capture());
        Query actualQuery = captor.getValue();
        assertThat(actualQuery.getPartitionKey())
                .isEqualTo("index#collection.name#name");
        assertThat(actualQuery.getPredicate())
                .matches(p -> !p.isRange())
                .matches(p -> p.getValue().getValue1().equals(index.getCollection().getName()))
                .matches(p -> p.getValue().getValue2() == null);
    }

    @Test
    void testGetByCollectionNameAndName() {
        //given
        Index index = randomIndex();
        Document document = IndexService.fromIndexToMap(index);
        when(repoTable.query(any())).thenReturn(
                new QueryResultsWithCursor(generator.objects(RecordBuilder.class, 2)
                        .map(b -> b.withDocument(document)
                                .build()).collect(toList()),
                        null)
        );
        //when
        Optional<Index> result = indexService.getByCollectionNameAndName(index.getCollection().getName(), index.getName());
        //then
        assertThat(result)
                .get()
                .usingComparator(new FieldByFieldComparator())
                .isEqualToIgnoringGivenFields(index, "collection");
        ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
        verify(repoTable).query(captor.capture());
        Query actualQuery = captor.getValue();
        assertThat(actualQuery.getPartitionKey())
                .isEqualTo("index#collection.name#name");
        assertThat(actualQuery.getPredicate())
                .matches(p -> !p.isRange())
                .matches(p -> p.getValue().getValue1().equals(index.getCollection().getName() + "#" + index.getName()))
                .matches(p -> p.getValue().getValue2() == null);
    }

    @Test
    void testDeleteIndexById() {
        //given
        UUID id = UUID.randomUUID();
        Index index = randomIndex();
        Document document = IndexService.fromIndexToMap(index);
        doNothing().when(repoTable).delete(any(), any());
        //when
        indexService.deleteIndexById(id);
        //then
        verify(repoTable).delete(eq("index#" + id), eq("index"));
    }


}
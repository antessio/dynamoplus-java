package antessio.dynamoplus.service;

import antessio.dynamoplus.dynamodb.bean.Query;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.RecordBuilder;
import antessio.dynamoplus.dynamodb.bean.query.QueryResultsWithCursor;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.system.bean.collection.Attribute;
import antessio.dynamoplus.system.bean.collection.AttributeBuilder;
import antessio.dynamoplus.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.system.bean.index.Index;
import antessio.dynamoplus.system.bean.index.IndexBuilder;
import org.assertj.core.internal.FieldByFieldComparator;
import org.jeasy.random.EasyRandom;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;


import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class IndexServiceTest {
    private DynamoDbTableRepository repoTable = mock(DynamoDbTableRepository.class);
    private IndexService indexService;
    private EasyRandom generator;

    @BeforeEach
    void setUp() {
        indexService = new IndexService(repoTable);
        generator = new EasyRandom();

    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(repoTable);
    }

    @Test
    void createIndex() {
        //given
        Index index = randomIndex();
        Map<String, Object> document = IndexService.fromIndexToMap(index);
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
    void createGsiRows() {
        //given
        Index index = randomIndex();
        Map<String, Object> document = IndexService.fromIndexToMap(index);
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
    void getById() {
        //given
        UUID id = UUID.randomUUID();
        Index index = randomIndex();
        Map<String, Object> document = IndexService.fromIndexToMap(index);
        when(repoTable.get(any(), any())).thenReturn(generator.nextObject(RecordBuilder.class)
                .withDocument(document)
                .build());
        //when
        Index result = indexService.getById(id);
        //then
        assertThat(result)
                .usingComparator(new FieldByFieldComparator())
                .isEqualToIgnoringGivenFields(index, "collection");
        verify(repoTable).get(eq("index#" + id), eq("index"));
    }

    @Test
    void getByCollectionName() {
        //given
        UUID id = UUID.randomUUID();
        Index index = randomIndex();
        Map<String, Object> document = IndexService.fromIndexToMap(index);
        when(repoTable.query(any())).thenReturn(
                new QueryResultsWithCursor(generator.objects(RecordBuilder.class, 2)
                        .map(b -> b.withDocument(document)
                                .build()).collect(toList()),
                        null)
        );
        //when
        Optional<Index> result = indexService.getByCollectionName("example");
        //then
        assertThat(result)
                .get()
                .usingComparator(new FieldByFieldComparator())
                .isEqualToIgnoringGivenFields(index, "collection");
        ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
        verify(repoTable).query(captor.capture());
        assertThat(captor.getValue().getPartitionKey())
                .isEqualTo("index#collection.name#name");
    }

    @Test
    void getByCollectionNameAndName() {
        //given
        //when
        //then
    }

    @Test
    void deleteIndexById() {
        //given
        //when
        //then
    }

    private Index randomIndex() {
        return generator.nextObject(IndexBuilder.class)
                .collection(generator.nextObject(CollectionBuilder.class)
                        .fields(generator.objects(AttributeBuilder.class, 3)
                                .map(b -> b.attributes(null).build())
                                .collect(toList()))
                        .createCollection())
                .conditions(generator.objects(String.class, 3).collect(toList()))
                .createIndex();
    }
}
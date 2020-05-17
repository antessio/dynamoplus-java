package antessio.dynamoplus.service;

import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.RecordBuilder;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.system.bean.index.Index;
import antessio.dynamoplus.system.bean.index.IndexBuilder;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.jeasy.random.EasyRandom;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class IndexServiceTest {
    private DynamoDbTableRepository repoTable = mock(DynamoDbTableRepository.class);
    private IndexService indexService;
    private EasyRandom generator;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        indexService = new IndexService(repoTable);
        generator = new EasyRandom();
        objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(repoTable);
    }

    @Test
    void createIndex() {
        //given
        Index index = generator.nextObject(Index.class);
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
        Index index = generator.nextObject(Index.class);
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
    void fromMapToIndex() {
        //given
        Index index = generator.nextObject(IndexBuilder.class)
                .collection("example")
                .createIndex();
        Map<String, Object> indexAsMap = objectMapper.convertValue(index, new TypeReference<Map<String, Object>>() {
        });
        //when
        Index result = IndexService.fromMapToIndex(indexAsMap);
        //then
        assertThat(result)
                .isEqualToIgnoringGivenFields(index, "collection");
        assertThat(result.getCollection())
                .isEqualToIgnoringGivenFields(index.getCollection(), "attributes", "autoGenerateId");
    }

    @Test
    void fromIndexToMap() {
        //given
        Index index = generator.nextObject(IndexBuilder.class)
                .collection(new CollectionBuilder()
                        .name("example")
                        .idKey("id")
                        .createCollection())
                .conditions(Arrays.asList(
                        "field1", "field2"
                ))
                .createIndex();
        Map<String, Object> indexAsMap = objectMapper.convertValue(index, new TypeReference<Map<String, Object>>() {
        });
        //when
        Map<String, Object> result = IndexService.fromIndexToMap(index);
        //then
        assertThat(result).isEqualTo(indexAsMap);
    }

    @Test
    void getById() {
        //given
        //when
        //then
    }

    @Test
    void getByCollectionName() {
        //given
        //when
        //then
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
}
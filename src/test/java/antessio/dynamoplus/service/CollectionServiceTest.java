package antessio.dynamoplus.service;

import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.RecordBuilder;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.system.bean.collection.*;
import antessio.dynamoplus.system.bean.collection.Collection;
import org.assertj.core.groups.Tuple;
import org.jeasy.random.EasyRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class CollectionServiceTest {

    private EasyRandom generator;
    private CollectionService collectionService;
    private DynamoDbTableRepository tableRepository = mock(DynamoDbTableRepository.class);

    @BeforeEach
    void setUp() {
        generator = new EasyRandom();
        collectionService = new CollectionService(tableRepository);
    }

    @Test
    void testGetCollectionByName() {
        //given
        Collection collection = randomCollection();
        Map<String, Object> document = CollectionService.fromCollectionToMap(collection);
        when(tableRepository.get(any(), any())).thenReturn(generator.nextObject(RecordBuilder.class)
                .withDocument(document)
                .build());
        //when
        Collection result = collectionService.getCollectionByName(collection.getName());
        //then
        assertThat(result).isEqualToIgnoringGivenFields(collection, "attributes");
        assertThat(result.getAttributes())
                .extracting(a -> tuple(a.getName(), a.getType(), a.getConstraints()))
                .containsOnly(collection.getAttributes().stream().map(a -> tuple(a.getName(), a.getType(), a.getConstraints())).toArray(Tuple[]::new));
        verify(tableRepository).get(eq("collection#" + collection.getName()), eq("collection"));
    }

    @Test
    void testInsertNewCollection() {
        //given
        Collection collection = randomCollection();
        Map<String, Object> document = CollectionService.fromCollectionToMap(collection);
        Record expectedRecord = RecordBuilder.aRecord()
                .withPk("collection#" + collection.getName())
                .withSk("collection")
                .withData(collection.getName())
                .withDocument(document)
                .build();
        when(tableRepository.create(any())).thenAnswer(returnsFirstArg());
        //when
        Collection result = collectionService.createCollection(collection);
        //then
        assertThat(result).isEqualToIgnoringGivenFields(collection, "attributes");
        assertThat(result.getAttributes())
                .extracting(a -> tuple(a.getName(), a.getType(), a.getConstraints()))
                .containsOnly(collection.getAttributes().stream().map(a -> tuple(a.getName(), a.getType(), a.getConstraints())).toArray(Tuple[]::new));
        verify(tableRepository).create(refEq(expectedRecord));
    }

    @Test
    void testUpdateCollection() {
        //given
        Collection collection = randomCollection();
        Map<String, Object> document = CollectionService.fromCollectionToMap(collection);
        Record expectedRecord = RecordBuilder.aRecord()
                .withPk("collection#" + collection.getName())
                .withSk("collection")
                .withData(collection.getName())
                .withDocument(document)
                .build();
        when(tableRepository.update(any())).thenAnswer(returnsFirstArg());
        //when
        Collection result = collectionService.updateCollection(collection);
        //then
        assertThat(result).isEqualToIgnoringGivenFields(collection, "attributes");
        assertThat(result.getAttributes())
                .extracting(a -> tuple(a.getName(), a.getType(), a.getConstraints()))
                .containsOnly(collection.getAttributes().stream().map(a -> tuple(a.getName(), a.getType(), a.getConstraints())).toArray(Tuple[]::new));
        verify(tableRepository).update(refEq(expectedRecord));
    }

    @Test
    void testDeleteCollectionByName() {
        //given
        Collection collection = randomCollection();
        Map<String, Object> document = CollectionService.fromCollectionToMap(collection);
        when(tableRepository.delete(any(), any())).thenReturn(generator.nextObject(RecordBuilder.class)
                .withDocument(document)
                .build());
        //when
        collectionService.delete(collection.getName());
        //then
        verify(tableRepository).delete(eq("collection#" + collection.getName()), eq("collection"));
    }

    private Collection randomCollection() {
        return generator.nextObject(CollectionBuilder.class)
                .attributes(generator.objects(AttributeBuilder.class, 4)
                        .map(b -> b.constraints(
                                generator.objects(CollectionAttributeConstraint.class, 1)
                                        .collect(toList()))
                                .attributes(Collections.emptyList())
                                .build())
                        .collect(toList())
                )
                .createCollection();
    }


}
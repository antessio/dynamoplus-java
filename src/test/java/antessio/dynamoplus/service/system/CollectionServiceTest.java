package antessio.dynamoplus.service.system;

import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.RecordBuilder;
import antessio.dynamoplus.dynamodb.impl.DynamoDbTableRepository;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.system.bean.collection.AttributeBuilder;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.collection.CollectionAttributeConstraint;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
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
        Document document = CollectionService.fromCollectionToMap(collection);
        when(tableRepository.get(any(), any())).thenReturn(Optional.of(generator.nextObject(RecordBuilder.class)
                .withDocument(document)
                .build()));
        //when
        Optional<Collection> result = collectionService.getCollectionByName(collection.getName());
        //then
        assertThat(result).get().isEqualToIgnoringGivenFields(collection, "attributes");
        assertThat(result.get().getAttributes())
                .extracting(a -> tuple(a.getName(), a.getType(), a.getConstraints()))
                .containsOnly(collection.getAttributes().stream().map(a -> tuple(a.getName(), a.getType(), a.getConstraints())).toArray(Tuple[]::new));
        verify(tableRepository).get(eq("collection#" + collection.getName()), eq("collection"));
    }

    @Test
    void testInsertNewCollection() {
        //given
        Collection collection = randomCollection();
        Document document = CollectionService.fromCollectionToMap(collection);
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
        Document document = CollectionService.fromCollectionToMap(collection);
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
        Document document = CollectionService.fromCollectionToMap(collection);
        doNothing().when(tableRepository).delete(any(), any());
        //when
        collectionService.delete(collection.getName());
        //then
        verify(tableRepository).delete(eq("collection#" + collection.getName()), eq("collection"));
    }

    private Collection randomCollection() {
        return generator.nextObject(CollectionBuilder.class)
                .withAttributes(generator.objects(AttributeBuilder.class, 4)
                        .map(b -> b.constraints(
                                generator.objects(CollectionAttributeConstraint.class, 1)
                                        .collect(toList()))
                                .attributes(Collections.emptyList())
                                .build())
                        .collect(toList())
                )
                .build();
    }


}
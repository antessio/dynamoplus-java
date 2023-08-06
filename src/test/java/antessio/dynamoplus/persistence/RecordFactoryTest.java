package antessio.dynamoplus.persistence;

import antessio.dynamoplus.persistence.bean.Record;
import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.service.system.bean.index.Index;
import antessio.dynamoplus.service.system.bean.index.IndexBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Arrays;

import static antessio.dynamoplus.utils.MapUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

public class RecordFactoryTest {

    private RecordFactory recordFactory;

    @BeforeEach
    void setUp() {
        recordFactory = RecordFactory.getInstance();
    }

    @Test
    void shouldGetMasterRecordWithNoOrderUnique() {
        //given
        Document document = ofDocument(
                entry("field1", "value1"),
                entry("field2", "value2"),
                entry("field3", ofDocument(
                        entry("field31", "value31"),
                        entry("field32", ofDocument(
                                entry("field321", "value321")
                        ))
                ))
        );

        String expectedCollectionName = "example";
        Collection collection = CollectionBuilder.aCollection()
                .withName(expectedCollectionName)
                .withIdKey("field2")
                .build();
        //when
        Record record = recordFactory.masterRecordFromDocument(document, collection);
        //then
        assertThat(record)
                .matches(r -> r.getRecordKey().getPk().equals(expectedCollectionName + "#value2"))
                .matches(r -> r.getRecordKey().getSk().equals(expectedCollectionName))
                .matches(r -> r.getData().equals("value2"))
                .matches(r -> r.getDocument().equals(document));
    }

    @Test
    void shouldGetMasterRecordWithOrderUnique() {
        //given
        Long expectedOrderUnique = 100L;
        Document document = ofDocument(
                entry("field1", "value1"),
                entry("field2", "value2"),
                entry("order_unique", expectedOrderUnique),
                entry("field3", ofDocument(
                        entry("field31", "value31"),
                        entry("field32", ofDocument(
                                entry("field321", "value321")
                        ))
                ))
        );

        String expectedCollectionName = "example";
        Collection collection = CollectionBuilder.aCollection()
                .withName(expectedCollectionName)
                .withIdKey("field2")
                .build();
        //when
        Record record = recordFactory.masterRecordFromDocument(document, collection);
        //then
        assertThat(record)
                .matches(r -> r.getRecordKey().getPk().equals(expectedCollectionName + "#value2"))
                .matches(r -> r.getRecordKey().getSk().equals(expectedCollectionName))
                .hasFieldOrPropertyWithValue("data", expectedOrderUnique + "")
                .matches(r -> r.getDocument().equals(document));
    }

    @Test
    void shouldGetIndexingRecord() {
        //given
        Long expectedOrderUnique = 100L;
        Document document = ofDocument(
                entry("field1", "value1"),
                entry("field2", "value2"),
                entry("order_unique", expectedOrderUnique),
                entry("field3", ofEntries(
                        entry("field31", "value31"),
                        entry("field32", ofEntries(
                                entry("field321", "value321")
                        ))
                ))
        );

        String expectedCollectionName = "example";
        Collection collection = CollectionBuilder.aCollection()
                .withName(expectedCollectionName)
                .withIdKey("field2")
                .build();
        String[] expectedFields = {"field1", "field3.field31", "field3.field32.field321"};
        Index index = IndexBuilder.anIndex()
                .withCollection(collection)
                .withConditions(Arrays.asList(expectedFields))
                .build();
        //when
        Record record = recordFactory.indexingRecordFromDocument(document, index);
        //then
        assertThat(record)
                .matches(r -> r.getRecordKey().getPk().equals(expectedCollectionName + "#value2"))
                .matches(r -> r.getRecordKey().getSk().equals(String.format("%s#%s", expectedCollectionName, String.join("#", expectedFields))))
                .hasFieldOrPropertyWithValue("data", "value1#value31#value321")
                .matches(r -> r.getDocument().equals(document));
    }

    @Test
    void shouldGetIndexingRecordWithOrderingKey() {
        //given
        Long expectedOrderUnique = 100L;
        Document document = ofDocument(
                entry("field1", "value1"),
                entry("field2", "value2"),
                entry("order_unique", expectedOrderUnique),
                entry("field3", ofEntries(
                        entry("field31", "value31"),
                        entry("field32", ofEntries(
                                entry("field321", "value321")
                        ))
                ))
        );

        String expectedCollectionName = "example";
        Collection collection = CollectionBuilder.aCollection()
                .withName(expectedCollectionName)
                .withIdKey("field2")
                .build();
        String[] expectedFields = {"field1", "field3.field31", "field3.field32.field321"};
        Index index = IndexBuilder.anIndex()
                .withCollection(collection)
                .withConditions(Arrays.asList(expectedFields))
                .withOrderingKey("order_unique")
                .build();
        //when
        Record record = recordFactory.indexingRecordFromDocument(document, index);
        //then
        assertThat(record)
                .matches(r -> r.getRecordKey().getPk().equals(expectedCollectionName + "#value2"))
                .matches(r -> r.getRecordKey().getSk().equals(String.format("%s#%s", expectedCollectionName, String.join("#", expectedFields))))
                .hasFieldOrPropertyWithValue("data", "value1#value31#value321#" + expectedOrderUnique)
                .matches(r -> r.getDocument().equals(document));
    }

    private Document ofDocument(AbstractMap.SimpleEntry<String, Object>... values) {
        return new Document(ofEntries(values), id);
    }
}

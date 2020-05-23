package antessio.dynamoplus.dynamodb;

import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import antessio.dynamoplus.service.system.bean.collection.CollectionBuilder;
import antessio.dynamoplus.service.system.bean.index.Index;
import antessio.dynamoplus.service.system.bean.index.IndexBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static antessio.dynamoplus.utils.MapUtil.entry;
import static antessio.dynamoplus.utils.MapUtil.ofEntries;
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
        Map<String, Object> document = ofEntries(
                entry("field1", "value1"),
                entry("field2", "value2"),
                entry("field3", ofEntries(
                        entry("field31", "value31"),
                        entry("field32", ofEntries(
                                entry("field321", "value321")
                        ))
                ))
        );

        String expectedCollectionName = "example";
        Collection collection = new CollectionBuilder()
                .name(expectedCollectionName)
                .idKey("field2")
                .createCollection();
        //when
        Record record = recordFactory.masterRecordFromDocument(document, collection);
        //then
        assertThat(record)
                .matches(r -> r.getPk().equals(expectedCollectionName + "#value2"))
                .matches(r -> r.getSk().equals(expectedCollectionName))
                .matches(r -> r.getData().equals("value2"))
                .matches(r -> r.getDocument().equals(document));
    }

    @Test
    void shouldGetMasterRecordWithOrderUnique() {
        //given
        Long expectedOrderUnique = 100L;
        Map<String, Object> document = ofEntries(
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
        Collection collection = new CollectionBuilder()
                .name(expectedCollectionName)
                .idKey("field2")
                .createCollection();
        //when
        Record record = recordFactory.masterRecordFromDocument(document, collection);
        //then
        assertThat(record)
                .matches(r -> r.getPk().equals(expectedCollectionName + "#value2"))
                .matches(r -> r.getSk().equals(expectedCollectionName))
                .hasFieldOrPropertyWithValue("data", expectedOrderUnique + "")
                .matches(r -> r.getDocument().equals(document));
    }

    @Test
    void shouldGetIndexingRecord() {
        //given
        Long expectedOrderUnique = 100L;
        Map<String, Object> document = ofEntries(
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
        Collection collection = new CollectionBuilder()
                .name(expectedCollectionName)
                .idKey("field2")
                .createCollection();
        String[] expectedFields = {"field1", "field3.field31", "field3.field32.field321"};
        Index index = new IndexBuilder()
                .collection(collection)
                .conditions(Arrays.asList(expectedFields))
                .createIndex();
        //when
        Record record = recordFactory.indexingRecordFromDocument(document, index);
        //then
        assertThat(record)
                .matches(r -> r.getPk().equals(expectedCollectionName + "#value2"))
                .matches(r -> r.getSk().equals(String.format("%s#%s", expectedCollectionName, String.join("#", expectedFields))))
                .hasFieldOrPropertyWithValue("data", "value1#value31#value321")
                .matches(r -> r.getDocument().equals(document));
    }

    @Test
    void shouldGetIndexingRecordWithOrderingKey() {
        //given
        Long expectedOrderUnique = 100L;
        Map<String, Object> document = ofEntries(
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
        Collection collection = new CollectionBuilder()
                .name(expectedCollectionName)
                .idKey("field2")
                .createCollection();
        String[] expectedFields = {"field1", "field3.field31", "field3.field32.field321"};
        Index index = new IndexBuilder()
                .collection(collection)
                .conditions(Arrays.asList(expectedFields))
                .orderingKey("order_unique")
                .createIndex();
        //when
        Record record = recordFactory.indexingRecordFromDocument(document, index);
        //then
        assertThat(record)
                .matches(r -> r.getPk().equals(expectedCollectionName + "#value2"))
                .matches(r -> r.getSk().equals(String.format("%s#%s", expectedCollectionName, String.join("#", expectedFields))))
                .hasFieldOrPropertyWithValue("data", "value1#value31#value321#" + expectedOrderUnique)
                .matches(r -> r.getDocument().equals(document));
    }
}

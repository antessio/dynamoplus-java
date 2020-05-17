package antessio.dynamoplus.common.service;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import java.util.List;
import java.util.Map;

import static antessio.dynamoplus.common.service.MapUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

public class IndexingUtilsTest {

    @Test
    void testFromConditionsToValues() {
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
        List<String> fields = Arrays.asList("field1", "field3.field31", "field3.field32.field321");
        //when
        Map<String, Object> result = IndexingUtils.getIndexingMap(document, fields);
        //then
        Map<String, Object> expectedMap = ofEntries(
                entry("field1", "value1"),
                entry("field3.field31", "value31"),
                entry("field3.field32.field321", "value321")
        );
        assertThat(result)
                .containsKeys(fields.toArray(new String[0]))
                .isEqualTo(expectedMap);
    }
}

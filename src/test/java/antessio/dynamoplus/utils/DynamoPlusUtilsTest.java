package antessio.dynamoplus.utils;

import antessio.dynamoplus.service.bean.Document;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static antessio.dynamoplus.utils.MapUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

public class DynamoPlusUtilsTest {

    public static final Document DOCUMENT_EXAMPLE = new Document(ofEntries(
            entry("field1", "value1"),
            entry("field2", "value2"),
            entry("field3", ofEntries(
                    entry("field31", "value31"),
                    entry("field32", ofEntries(
                            entry("field321", "value321")
                    ))
            ))
    )
    );

    @Test
    void shouldGetValueFromDocument() {
        //given
        String fieldName = "field3.field31";
        //when
        Optional<Object> result = DynamoPlusUtils.getValueRecursively(fieldName, DOCUMENT_EXAMPLE);
        //then
        assertThat(result)
                .get()
                .isEqualTo("value31");
    }

    @Test
    void testFromConditionsToValues() {
        //given
        List<String> fields = Arrays.asList("field1", "field3.field31", "field3.field32.field321");
        //when
        Map<String, String> result = DynamoPlusUtils.getIndexingMap(DOCUMENT_EXAMPLE, fields);
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

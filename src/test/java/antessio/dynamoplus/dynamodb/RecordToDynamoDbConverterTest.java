package antessio.dynamoplus.dynamodb;

import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.RecordBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static antessio.dynamoplus.utils.MapUtil.entry;
import static antessio.dynamoplus.utils.MapUtil.ofEntries;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class RecordToDynamoDbConverterTest {

    @Test
    void toDynamo() {
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
        Record record = RecordBuilder.aRecord()
                .withPk("test#12314")
                .withSk("test")
                .withData("12141241")
                .withDocument(document)
                .build();
        //when
        Map<String, AttributeValue> result = RecordToDynamoDbConverter.toDynamo(record);
        //then
        assertThat(result.get("pk").getS()).isEqualTo(record.getPk());
        assertThat(result.get("sk").getS()).isEqualTo(record.getSk());
        assertThat(result.get("data").getS()).isEqualTo(record.getData());
        Map<String, AttributeValue> documentDynamo = result.get("document").getM();
        assertThat(documentDynamo.get("field1").getS()).isEqualTo("value1");
        assertThat(documentDynamo.get("field3").getM())
                .isNotNull()
                .matches(m -> m.containsKey("field31"))
                .matches(m -> m.containsKey("field32"))
                .matches(m -> m.get("field31").getS().equals("value31"));
        assertThat(documentDynamo.get("field3").getM().get("field32").getM())
                .isNotNull()
                .matches(m -> m.get("field321").getS().equals("value321"));
    }

    @Test
    void fromDynamo() {
        //given
        String expectedPk = "test#1234";
        String expectedSk = "test";
        String expectedData = "1234";
        Map<String, Object> expectedDocument = ofEntries(
                entry("field1", "value1"),
                entry("field3", ofEntries(
                        entry("field31", "value31"),
                        entry("field32", ofEntries(
                                entry("field321", "value321")
                        ))
                ))
        );
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("pk", new AttributeValue().withS(expectedPk));
        item.put("sk", new AttributeValue().withS(expectedSk));
        item.put("data", new AttributeValue().withS(expectedData));
        Map<String, AttributeValue> nested1 = new HashMap<>();
        nested1.put("field1", new AttributeValue().withS("value1"));
        Map<String, AttributeValue> nested2 = new HashMap<>();
        nested2.put("field31", new AttributeValue().withS("value31"));
        Map<String, AttributeValue> nested3 = new HashMap<>();
        nested3.put("field321", new AttributeValue().withS("value321"));
        nested2.put("field32", new AttributeValue().withM(nested3));
        nested1.put("field3", new AttributeValue().withM(nested2));
        item.put("document", new AttributeValue().withM(nested1));
        //when
        Record result = RecordToDynamoDbConverter.fromDynamo(item);
        //then
        assertThat(result)
                .matches(r -> r.getPk().equals(expectedPk))
                .matches(r -> r.getSk().equals(expectedSk))
                .matches(r -> r.getData().equals(expectedData))
                .matches(r -> r.getDocument().equals(expectedDocument))
        ;

    }
}
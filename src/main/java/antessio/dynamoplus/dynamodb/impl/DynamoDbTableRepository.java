package antessio.dynamoplus.dynamodb.impl;

import antessio.dynamoplus.dynamodb.DynamoDb;
import antessio.dynamoplus.dynamodb.DynamoDbRepository;
import antessio.dynamoplus.dynamodb.RecordToDynamoDbConverter;
import antessio.dynamoplus.dynamodb.bean.Query;
import antessio.dynamoplus.dynamodb.bean.Record;
import antessio.dynamoplus.dynamodb.bean.RecordBuilder;
import antessio.dynamoplus.dynamodb.bean.query.*;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryResult;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DynamoDbTableRepository implements DynamoDbRepository {
    public static final String SK_DATA_INDEX = "sk-data-index";
    private final DynamoDb dynamoDb;
    private final String tableName;

    public DynamoDbTableRepository(DynamoDb dynamoDb, String tableName) {
        this.dynamoDb = dynamoDb;
        this.tableName = tableName;
    }

    @Override
    public Record create(Record r) {
        return Optional.of(r)
                .map(RecordToDynamoDbConverter::toDynamo)
                .map(item -> dynamoDb.insert(item, tableName))
                .map(RecordToDynamoDbConverter::fromDynamo)
                .orElseThrow(() -> new RuntimeException("invalid record"));

    }

    @Override
    public Record update(Record r) {
        return Optional.of(r)
                .map(RecordToDynamoDbConverter::toDynamoUpdate)
                .map(item -> dynamoDb.update(item.getKey(), item.getValue(), tableName))
                .map(RecordToDynamoDbConverter::fromDynamo)
                .orElseThrow(() -> new RuntimeException("invalid record"));
    }

    @Override
    public Optional<Record> get(String pk, String sk) {
        return Optional.of(RecordBuilder.aRecord()
                .withPk(pk)
                .withSk(sk)
                .build())
                .map(RecordToDynamoDbConverter::toDynamo)
                .map(key -> dynamoDb.getItem(key, tableName))
                .map(RecordToDynamoDbConverter::fromDynamo);
    }

    @Override
    public void delete(String pk, String sk) {
        Optional.of(RecordBuilder.aRecord()
                .withPk(pk)
                .withSk(sk)
                .build())
                .map(RecordToDynamoDbConverter::toDynamo)
                .ifPresent(key -> dynamoDb.delete(key, tableName));
    }

    @Override
    public QueryResultsWithCursor query(Query query) {
        Map<String, Condition> condition = new HashMap<>();
        condition.put("sk", new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withS(query.getPartitionKey())));
        if (query.getPredicate() instanceof And) {
            And and = (And) query.getPredicate();
            PredicateValue values = and.getValue();

            if (values.getValue2() != null) {
                condition.put("data", new Condition()
                        .withComparisonOperator(ComparisonOperator.BETWEEN)
                        .withAttributeValueList(
                                new AttributeValue()
                                        .withS(values.getValue1()),
                                new AttributeValue()
                                        .withS(values.getValue2())));
            } else {
                condition.put("data", new Condition()
                        .withComparisonOperator(ComparisonOperator.BEGINS_WITH)
                        .withAttributeValueList(
                                new AttributeValue()
                                        .withS(values.getValue1()))
                );
            }

        } else if (query.getPredicate() instanceof Eq) {
            Eq eq = (Eq) query.getPredicate();
            condition.put("data", new Condition()
                    .withComparisonOperator(ComparisonOperator.BEGINS_WITH)
                    .withAttributeValueList(new AttributeValue()
                            .withS(eq.getFieldValue())));

        } else if (query.getPredicate() instanceof Range) {
            Range range = (Range) query.getPredicate();
            condition.put("data", new Condition()
                    .withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(
                            new AttributeValue()
                                    .withS(range.getFrom()),
                            new AttributeValue()
                                    .withS(range.getTo())));

        }

        Map<String, AttributeValue> exclusiveStartKey = Optional.ofNullable(query.getStartFrom())
                .map(RecordToDynamoDbConverter::toDynamo)
                .orElse(null);
        QueryResult queryResult = dynamoDb.query(condition, exclusiveStartKey, query.getLimit(), SK_DATA_INDEX, tableName);
        return new QueryResultsWithCursor(
                queryResult.getItems()
                        .stream()
                        .map(RecordToDynamoDbConverter::fromDynamo)
                        .collect(Collectors.toList()),
                Optional.ofNullable(queryResult.getLastEvaluatedKey())
                        .map(RecordToDynamoDbConverter::fromDynamo)
                        .orElse(null)
        );

    }
}

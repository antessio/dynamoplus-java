package antessio.dynamoplus.common.query;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class And implements Predicate {
    private List<Predicate> and;

    public And() {
    }

    public And(List<Predicate> and) {
        this.and = and;
    }

    public List<Predicate> getAnd() {
        return and;
    }


    @Override
    public String toString() {
        return "And{" +
                "and=" + and +
                '}';
    }

    @Override
    public boolean isRange() {
        return and
                .stream()
                .map(Predicate::isRange)
                .reduce(Boolean::logicalAnd)
                .orElse(false);
    }

    @Override
    public PredicateValue getValue() {
        if (isRange()) {
            Map<Boolean, List<Predicate>> tempMap = and
                    .stream()
                    .filter(predicate -> !(predicate instanceof And))
                    .collect(Collectors.groupingBy(Predicate::isRange));
            String prefix = fromPredicateListToString(tempMap.get(false));
            Optional<PredicateValue> values = tempMap.get(true)
                    .stream()
                    .findFirst()
                    .map(Predicate::getValue);
            return values.orElseGet(() -> new PredicateValue(prefix, null));
        } else {
            return new PredicateValue(fromPredicateListToString(and), null);
        }
    }

    private String fromPredicateListToString(List<Predicate> predicateList) {
        return predicateList
                .stream()
                .filter(predicate -> !(predicate instanceof And))
                .map(Predicate::getValue)
                .map(PredicateValue::getValue1)
                .collect(Collectors.joining("#"));
    }
}

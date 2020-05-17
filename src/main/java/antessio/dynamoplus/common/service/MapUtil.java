package antessio.dynamoplus.common.service;

import static java.util.stream.Collectors.toMap;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

public class MapUtil {

    private MapUtil() {
    }

    @SafeVarargs
    public static Map<String, Object> ofEntries(SimpleEntry<String, Object>... values) {
        return Stream.of(values).collect(toMap(Entry::getKey, Entry::getValue));
    }

    public static SimpleEntry<String, Object> entry(String key, Object value) {
        return new SimpleEntry<String, Object>(key, value);
    }
}

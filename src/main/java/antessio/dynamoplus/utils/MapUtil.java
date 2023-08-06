package antessio.dynamoplus.utils;

import static java.util.stream.Collectors.toMap;

import java.util.AbstractMap.SimpleEntry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapUtil {

    private MapUtil() {
    }

    @SafeVarargs
    public static Map<String, Object> ofEntries(SimpleEntry<String, Object>... values) {
        return Stream.of(values).collect(toMap(Entry::getKey, Entry::getValue));
    }

    public static Map<String, Object> merge(Map<String, Object> source, Map<String, Object> toMergeInto) {


        Map<String, Object> result = new java.util.HashMap<>(source);
        result.putAll(toMergeInto);
        return result;
    }

    public static LinkedHashMap<String, Object> linkedHashMapOfEntries(SimpleEntry<String, Object>... values) {
        return Stream.of(values).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new));
    }

    public static SimpleEntry<String, Object> entry(String key, Object value) {
        return new SimpleEntry<String, Object>(key, value);
    }

    public static <T> SimpleEntry<String, T> entry(String key, T value, Class<T> cls) {
        return new SimpleEntry<String, T>(key, value);
    }

}

package antessio.dynamoplus.utils;

import antessio.dynamoplus.service.bean.Document;

import java.util.*;

import static antessio.dynamoplus.utils.MapUtil.*;

/**
 * Utils for indexing documents and converting from documents to objects
 */
public final class DynamoPlusUtils {
    public static final String FIELD_SEPARATOR = ".";

    private DynamoPlusUtils() {

    }

    /**
     * Find the <i>fields</i> in <i>document</i> and builds a map where the key
     * is the field an the value is the once found in the document map.
     * As convention, to look into nested object fields should use the {@value DynamoPlusUtils#FIELD_SEPARATOR}
     *
     * @param document the document to store as a map
     * @param fields   the list of values to index
     * @return Map field - document value
     */
    public static Map<String, String> getIndexingMap(Document document, List<String> fields) {
        return linkedHashMapOfEntries(
                fields.stream()
                        .map(f -> entry(f, getValueRecursively(f, document)
                                .filter(Objects::nonNull)
                                .filter(v -> v instanceof String)
                                .orElse(null)))
                        .filter(e -> e.getValue() != null)
                        .toArray(AbstractMap.SimpleEntry[]::new)
        );
    }

    public static Optional<Object> getValueRecursively(String f, Document document) {
        String[] fieldsSplit = f.split("\\" + FIELD_SEPARATOR);
        if (fieldsSplit.length == 1) {
            return Optional.ofNullable(document.get(fieldsSplit[0]));
        } else {
            Object r1 = document.get(fieldsSplit[0]);
            if (r1 instanceof Map) {
                Map<String, Object> nestedMap = (Map<String, Object>) r1;
                if (nestedMap.containsKey(fieldsSplit[1])) {
                    return getValueRecursively(String.join(FIELD_SEPARATOR, Arrays.copyOfRange(fieldsSplit, 1, fieldsSplit.length)), new Document(nestedMap));
                }
            }
        }
        return Optional.empty();
    }


}

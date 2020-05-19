package antessio.dynamoplus.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import java.io.IOException;
import java.util.Map;

public final class ConversionUtils {
    private ObjectMapper objectMapper;
    private static ConversionUtils instance;

    private ConversionUtils() {
        objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    }

    public static ConversionUtils getInstance() {
        if (instance == null) {
            instance = new ConversionUtils();
        }
        return instance;
    }

    public <T> Map<String, Object> convertObject(T obj) {
        return objectMapper.convertValue(obj, new TypeReference<Map<String, Object>>() {
        });
    }

    public <T> T convertMap(Map<String, Object> m, Class<T> cls) {
        return objectMapper.convertValue(m, cls);
    }

    public String convertToJson(Map<String, Object> document) {
        try {
            return objectMapper.writeValueAsString(document);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("unable serialize document as json", e);
        }
    }

    public Map<String, Object> fromJson(String document) {
        try {
            return objectMapper.readValue(document, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            throw new RuntimeException("unable deserialize document as json", e);
        }
    }
}

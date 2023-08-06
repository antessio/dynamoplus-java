package antessio.dynamoplus.utils;

import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationApiKey;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationHttpSignature;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationInterface;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Map;

import static antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationInterface.*;

public final class ConversionUtils {

    private ObjectMapper objectMapper;
    private static ConversionUtils instance;

    private ConversionUtils() {
        objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ClientAuthorizationInterface.class, new ClientAuthorizationDeserializer(ClientAuthorizationInterface.class));
        objectMapper.registerModule(module);

    }

    public static ConversionUtils getInstance() {
        if (instance == null) {
            instance = new ConversionUtils();
        }
        return instance;
    }

    public <T> Document convertObject(T obj) {
        return new Document(
                objectMapper.convertValue(obj, new TypeReference<Map<String, Object>>() {
                }),
                null);
    }

    public <T> Document convertObject(T obj, String keyField) {
        Map<String, Object> dict = objectMapper.convertValue(obj, new TypeReference<Map<String, Object>>() {
        });
        return new Document(
                dict,
                (String) dict.get(keyField));
    }

    public <T> T convertDocument(Document document, Class<T> cls) {
        return convertMap(document.getDict(), cls);
    }

    public <T> T convertMap(Map<String, Object> m, Class<T> cls) {
        return objectMapper.convertValue(m, cls);
    }

    private String convertToJson(Map<String, Object> document) {
        try {
            return objectMapper.writeValueAsString(document);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("unable serialize document as json", e);
        }
    }

    public String convertToJson(Document document) {
        return convertToJson(document.getDict());
    }

    public Map<String, Object> fromJson(String document) {
        try {
            return objectMapper.readValue(document, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            throw new RuntimeException("unable deserialize document as json", e);
        }
    }

    private static class ClientAuthorizationDeserializer extends StdDeserializer<ClientAuthorizationInterface> {

        protected ClientAuthorizationDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public ClientAuthorizationInterface deserialize(
                JsonParser jsonParser,
                DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
            ObjectNode root = mapper.readTree(jsonParser);

            ClientAuthorizationType clientAuthorizationType = ClientAuthorizationType.valueOf(root.get("type").asText());
            switch (clientAuthorizationType) {
                case API_KEY:
                    return mapper.readValue(root.toString(), ClientAuthorizationApiKey.class);
                case HTTP_SIGNATURE:
                    return mapper.readValue(root.toString(), ClientAuthorizationHttpSignature.class);
            }
            return null;
        }

    }

}

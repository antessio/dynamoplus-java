package antessio.dynamoplus.service.exception;

import antessio.dynamoplus.service.bean.Document;
import antessio.dynamoplus.service.system.bean.collection.Collection;
import org.everit.json.schema.ValidationException;

import java.util.List;
import java.util.Map;

public class DocumentValidationException extends DynamoPlusException {
    private final Document document;
    private final List<String> allMessages;
    private final Collection collection;


    public DocumentValidationException(Document document, List<String> allMessages, Collection collection, ValidationException e) {
        super(String.format("Validation errors:\n%s", String.join("\n", allMessages)), e);
        this.document = document;
        this.allMessages = allMessages;
        this.collection = collection;
    }

    public List<String> getAllMessages() {
        return this.allMessages;
    }

    public Collection getCollection() {
        return collection;
    }

    public Document getDocument() {
        return document;
    }
}

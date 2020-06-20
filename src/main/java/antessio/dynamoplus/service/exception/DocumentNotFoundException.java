package antessio.dynamoplus.service.exception;

import antessio.dynamoplus.service.system.bean.collection.Collection;

public class DocumentNotFoundException extends DynamoPlusException {
    private final String id;
    private final Collection collection;

    public DocumentNotFoundException(String id, Collection collection) {
        super(String.format("document %s of collection %s not found", id, collection.getName()));
        this.id = id;
        this.collection = collection;
    }

    public String getId() {
        return id;
    }

    public Collection getCollection() {
        return collection;
    }
}

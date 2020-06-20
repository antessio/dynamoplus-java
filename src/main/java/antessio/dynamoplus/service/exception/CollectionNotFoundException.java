package antessio.dynamoplus.service.exception;

public class CollectionNotFoundException extends DynamoPlusException {

    public CollectionNotFoundException(String message) {
        super(message);
    }

    public CollectionNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}

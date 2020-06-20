package antessio.dynamoplus.service.exception;

public class DynamoPlusException extends RuntimeException {
    public DynamoPlusException(String message) {
        super(message);
    }

    public DynamoPlusException(String message, Throwable cause) {
        super(message, cause);
    }
}

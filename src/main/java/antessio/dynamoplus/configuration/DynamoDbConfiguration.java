package antessio.dynamoplus.configuration;

public class DynamoDbConfiguration {
    private String region;
    private String accessKey;
    private String secret;
    private String endpoint;

    public DynamoDbConfiguration(String region, String accessKey, String secret, String endpoint) {
        this.region = region;
        this.accessKey = accessKey;
        this.secret = secret;
        this.endpoint = endpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecret() {
        return secret;
    }

    public String getEndpoint() {
        return endpoint;
    }
}

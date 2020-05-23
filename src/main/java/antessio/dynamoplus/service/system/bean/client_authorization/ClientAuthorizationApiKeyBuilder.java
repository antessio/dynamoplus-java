package antessio.dynamoplus.service.system.bean.client_authorization;

import java.util.List;

public final class ClientAuthorizationApiKeyBuilder {
    private String apiKey;
    private List<String> whiteListHosts;
    private String clientId;
    private List<ClientAuthorizationScope> clientScopes;

    private ClientAuthorizationApiKeyBuilder() {
    }

    public static ClientAuthorizationApiKeyBuilder aClientAuthorizationApiKey() {
        return new ClientAuthorizationApiKeyBuilder();
    }

    public ClientAuthorizationApiKeyBuilder withApiKey(String apiKey) {
        this.apiKey = apiKey;
        return this;
    }

    public ClientAuthorizationApiKeyBuilder withWhiteListHosts(List<String> whiteListHosts) {
        this.whiteListHosts = whiteListHosts;
        return this;
    }

    public ClientAuthorizationApiKeyBuilder withClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public ClientAuthorizationApiKeyBuilder withClientScopes(List<ClientAuthorizationScope> clientScopes) {
        this.clientScopes = clientScopes;
        return this;
    }

    public ClientAuthorizationApiKey build() {
        return new ClientAuthorizationApiKey(clientId, clientScopes, apiKey, whiteListHosts);
    }
}

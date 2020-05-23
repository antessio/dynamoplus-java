package antessio.dynamoplus.service.system.bean.client_authorization;

public final class ClientAuthorizationScopeBuilder {
    private ClientAuthorizationScope.ScopeType scopeType;
    private String collectionName;

    private ClientAuthorizationScopeBuilder() {
    }

    public static ClientAuthorizationScopeBuilder aClientAuthorizationScope() {
        return new ClientAuthorizationScopeBuilder();
    }

    public ClientAuthorizationScopeBuilder withScopeType(ClientAuthorizationScope.ScopeType scopeType) {
        this.scopeType = scopeType;
        return this;
    }

    public ClientAuthorizationScopeBuilder withCollectionName(String collectionName) {
        this.collectionName = collectionName;
        return this;
    }

    public ClientAuthorizationScope build() {
        return new ClientAuthorizationScope(scopeType, collectionName);
    }
}

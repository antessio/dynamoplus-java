package antessio.dynamoplus.service.system.bean.client_authorization;

import java.util.List;

public final class ClientAuthorizationBuilder {
    private String clientId;
    private List<ClientAuthorizationScope> clientScopes;
    private ClientAuthorizationInterface.ClientAuthorizationType type;

    private ClientAuthorizationBuilder() {
    }

    public static ClientAuthorizationBuilder aClientAuthorization() {
        return new ClientAuthorizationBuilder();
    }

    public static ClientAuthorizationBuilder authorizationBuilder(ClientAuthorization clientAuthorization) {
        return new ClientAuthorizationBuilder()
                .withClientId(clientAuthorization.getClientId())
                .withClientScopes(clientAuthorization.getClientScopes())
                .withType(clientAuthorization.getType());

    }

    public ClientAuthorizationBuilder withClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public ClientAuthorizationBuilder withClientScopes(List<ClientAuthorizationScope> clientScopes) {
        this.clientScopes = clientScopes;
        return this;
    }

    public ClientAuthorizationBuilder withType(ClientAuthorizationInterface.ClientAuthorizationType type) {
        this.type = type;
        return this;
    }

    public ClientAuthorization build() {
        return new ClientAuthorization(clientId, clientScopes, type);
    }
}

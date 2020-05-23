package antessio.dynamoplus.service.system.bean.client_authorization;

import java.util.List;
import java.util.Objects;


public class ClientAuthorization implements ClientAuthorizationInterface {
    private String clientId;
    private List<ClientAuthorizationScope> clientScopes;
    private ClientAuthorizationType type;

    public ClientAuthorization() {
    }

    public ClientAuthorization(String clientId, List<ClientAuthorizationScope> clientScopes, ClientAuthorizationType type) {
        this.clientId = clientId;
        this.clientScopes = clientScopes;
        this.type = type;
    }

    public String getClientId() {
        return clientId;
    }


    public List<ClientAuthorizationScope> getClientScopes() {
        return clientScopes;
    }

    @Override
    public ClientAuthorizationType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientAuthorization that = (ClientAuthorization) o;
        return Objects.equals(clientId, that.clientId) &&
                Objects.equals(clientScopes, that.clientScopes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, clientScopes);
    }

    @Override
    public String toString() {
        return "ClientAuthorization{" +
                "clientId='" + clientId + '\'' +
                ", clientScopes=" + clientScopes +
                '}';
    }
}

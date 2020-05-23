package antessio.dynamoplus.service.system.bean.client_authorization;

import java.util.List;
import java.util.Objects;

public class ClientAuthorizationApiKey extends ClientAuthorization {
    private String apiKey;
    private List<String> whiteListHosts;

    public ClientAuthorizationApiKey() {
    }

    public ClientAuthorizationApiKey(String clientId, List<ClientAuthorizationScope> clientScopes, String apiKey, List<String> whiteListHosts) {
        super(clientId, clientScopes, ClientAuthorizationType.API_KEY);
        this.apiKey = apiKey;
        this.whiteListHosts = whiteListHosts;
    }

    public String getApiKey() {
        return apiKey;
    }

    public List<String> getWhiteListHosts() {
        return whiteListHosts;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ClientAuthorizationApiKey that = (ClientAuthorizationApiKey) o;
        return Objects.equals(apiKey, that.apiKey) &&
                Objects.equals(whiteListHosts, that.whiteListHosts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), apiKey, whiteListHosts);
    }

    @Override
    public String toString() {
        return "ClientAuthorizationApiKey{" +
                "apiKey='" + apiKey + '\'' +
                ", whiteListHosts=" + whiteListHosts +
                '}';
    }
}

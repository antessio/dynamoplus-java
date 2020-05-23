package antessio.dynamoplus.service.system.bean.client_authorization;

import java.util.List;

public class ClientAuthorizationHttpSignature extends ClientAuthorization {
    private String clientPublicKey;

    public ClientAuthorizationHttpSignature() {
    }

    public ClientAuthorizationHttpSignature(String clientId, List<ClientAuthorizationScope> clientScopes, String clientPublicKey) {
        super(clientId, clientScopes, ClientAuthorizationType.HTTP_SIGNATURE);
        this.clientPublicKey = clientPublicKey;
    }

    public String getClientPublicKey() {
        return clientPublicKey;
    }
}

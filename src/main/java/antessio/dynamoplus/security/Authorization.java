package antessio.dynamoplus.security;

import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorization;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationInterface;

public abstract class Authorization {
    private final String clientId;

    protected Authorization(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }

    public abstract boolean verify(ClientAuthorizationInterface clientAuthorization);

}

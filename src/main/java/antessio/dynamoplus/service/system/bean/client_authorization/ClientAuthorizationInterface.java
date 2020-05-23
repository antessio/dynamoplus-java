package antessio.dynamoplus.service.system.bean.client_authorization;

import java.util.List;

public interface ClientAuthorizationInterface {
    enum ClientAuthorizationType {
        HTTP_SIGNATURE,
        API_KEY
    }

    String getClientId();

    ClientAuthorizationType getType();

    List<ClientAuthorizationScope> getClientScopes();
}

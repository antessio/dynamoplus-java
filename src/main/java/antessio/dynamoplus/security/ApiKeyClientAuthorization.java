package antessio.dynamoplus.security;

import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorization;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationApiKey;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationInterface;

public class ApiKeyClientAuthorization extends Authorization {

    private final String apiKey;
    protected ApiKeyClientAuthorization(String clientId, String apiKey) {
        super(clientId);
        this.apiKey = apiKey;
    }

    public boolean verify(ClientAuthorizationApiKey clientAuthorization){
        //TODO: implement http signature verification
        return true;
    }


    @Override
    public boolean verify(ClientAuthorizationInterface clientAuthorization) {
        if (clientAuthorization instanceof ClientAuthorizationApiKey clientAuthorizationApiKey ){
            return this.verify(clientAuthorizationApiKey);
        }
        return false;
    }

}

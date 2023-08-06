package antessio.dynamoplus.security;

import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorization;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationHttpSignature;
import antessio.dynamoplus.service.system.bean.client_authorization.ClientAuthorizationInterface;

public class HttpSignatureClientAuthorization extends Authorization {

    private final String signature;

    protected HttpSignatureClientAuthorization(String clientId, String signature) {
        super(clientId);
        this.signature = signature;
    }

    public boolean verifySignature(ClientAuthorizationHttpSignature clientAuthorization) {
        //TODO: implement http signature verification
        return true;
    }


    @Override
    public boolean verify(ClientAuthorizationInterface clientAuthorization) {
        if (clientAuthorization instanceof ClientAuthorizationHttpSignature clientAuthorizationHttpSignature) {
            return this.verifySignature(clientAuthorizationHttpSignature);
        }
        return false;
    }

}

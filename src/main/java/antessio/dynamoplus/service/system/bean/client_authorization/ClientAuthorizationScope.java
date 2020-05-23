package antessio.dynamoplus.service.system.bean.client_authorization;

import java.util.Objects;

public class ClientAuthorizationScope {
    public enum ScopeType {
        QUERY,
        GET,
        CREATE,
        UPDATE,
        DELETE
    }

    private ScopeType scopeType;
    private String collectionName;

    public ClientAuthorizationScope() {
    }

    public ClientAuthorizationScope(ScopeType scopeType, String collectionName) {
        this.scopeType = scopeType;
        this.collectionName = collectionName;
    }

    public ScopeType getScopeType() {
        return scopeType;
    }

    public String getCollectionName() {
        return collectionName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientAuthorizationScope that = (ClientAuthorizationScope) o;
        return scopeType == that.scopeType &&
                Objects.equals(collectionName, that.collectionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scopeType, collectionName);
    }

    @Override
    public String toString() {
        return "ClientAuthorizationScope{" +
                "scopeType=" + scopeType +
                ", collectionName='" + collectionName + '\'' +
                '}';
    }
}

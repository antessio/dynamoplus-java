package antessio.dynamoplus.persistence.bean;

import java.util.Objects;

public class RecordKey {

    private String pk;
    private String sk;

    public RecordKey(String pk, String sk) {
        this.pk = pk;
        this.sk = sk;
    }

    public String getPk() {
        return pk;
    }

    public String getSk() {
        return sk;
    }


    @Override
    public String toString() {
        return "RecordKey{" +
               "pk='" + pk + '\'' +
               ", sk='" + sk + '\'' +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RecordKey recordKey = (RecordKey) o;

        if (!Objects.equals(pk, recordKey.pk)) {
            return false;
        }
        return Objects.equals(sk, recordKey.sk);
    }

    @Override
    public int hashCode() {
        int result = pk != null ? pk.hashCode() : 0;
        result = 31 * result + (sk != null ? sk.hashCode() : 0);
        return result;
    }

}

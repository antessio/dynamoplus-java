package antessio.dynamoplus.service.bean;

import java.util.List;
import java.util.Optional;

public class PaginatedResult<RESULT, NEXT_ID> {
    private List<RESULT> data;
    private boolean hasMore;

    private NEXT_ID nextId;



    public PaginatedResult(List<RESULT> data, boolean hasMore) {
        this.data = data;
        this.hasMore = hasMore;
        this.nextId = null;
    }

    public PaginatedResult(List<RESULT> data, boolean hasMore, NEXT_ID nextId) {
        this.data = data;
        this.hasMore = hasMore;
        this.nextId = nextId;
    }

    public List<RESULT> getData() {
        return data;
    }

    public boolean isHasMore() {
        return hasMore;
    }

    public Optional<NEXT_ID> getNextId() {
        return Optional.ofNullable(nextId);
    }

}

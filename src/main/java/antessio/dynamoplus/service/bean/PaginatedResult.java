package antessio.dynamoplus.service.bean;

import java.util.List;

public class PaginatedResult<T> {
    private List<T> data;
    private boolean hasMore;

    public PaginatedResult(List<T> data, boolean hasMore) {
        this.data = data;
        this.hasMore = hasMore;
    }

    public List<T> getData() {
        return data;
    }

    public boolean isHasMore() {
        return hasMore;
    }
}

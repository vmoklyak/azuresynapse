package com.example.azuresynapse.queryexecution;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class QueryResult {

    private long startAtMs;
    @Builder.Default
    private long finishAtMs = System.currentTimeMillis();;
    private Object result;
    @Builder.Default
    private boolean success = false;

    public long durationMs() {
        return finishAtMs - startAtMs;
    }

}

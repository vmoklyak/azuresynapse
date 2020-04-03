package com.example.azuresynapse.queryexecution;

import com.example.azuresynapse.queryexecution.QueryResult.QueryResultBuilder;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Builder
class QueryExecutionJob {

    private final int queriesNumber;
    private final ExecutorService executorService;
    private final ConnectionProvider connectionProvider;
    private final RandomQueryProvider randomQueryProvider;

    List<QueryResult> run() {
        log.info("Start query execution job");
        long startAtMs = System.currentTimeMillis();
        List<QueryResult> result = IntStream.range(0, queriesNumber)
                .mapToObj(index -> CompletableFuture.supplyAsync(() -> executeQuery(index), executorService))
                .collect(Collectors.toList()).stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
        log.info("Finish query execution job");
        log.info("Elapsed time: {} Ms", (System.currentTimeMillis() - startAtMs));
        return result;
    }

    private QueryResult executeQuery(int index) {
        QueryResultBuilder resultBuilder =
                QueryResult.builder().startAtMs(System.currentTimeMillis());
        try (
                Statement statement = connectionProvider.provide().createStatement();
                ResultSet resultSet = statement.executeQuery(randomQueryProvider.provide())
        ) {
            if (resultSet.next()) {
                resultBuilder.success(true);
                resultBuilder.result(resultSet.getObject(1));
            }
            log.info("Finish query execution: {}", index);
        } catch (Exception cause) {
            resultBuilder.result(cause);
            log.error(String.format("Error query execution: %d", index), cause);
        }
        return resultBuilder.build();
    }

}

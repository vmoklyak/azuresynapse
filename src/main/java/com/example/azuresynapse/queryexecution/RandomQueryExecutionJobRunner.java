package com.example.azuresynapse.queryexecution;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javafx.util.Pair;
import lombok.SneakyThrows;

public class RandomQueryExecutionJobRunner {

  public static void main(String[] args) {
    ExecutorService executorService = Executors.newFixedThreadPool(10);

    ThreadLocalConnectionProvider threadLocalConnectionProvider = new ThreadLocalConnectionProvider(
        "dburl",
        new Pair<>("LoaderXlargeRC", "pass"),
        new Pair<>("LoaderRC80", "pass"),
        new Pair<>("jbtadmin", "pass")
    );

    RandomQueryProvider randomQueryProvider = new RandomQueryProvider(
        Collections.singletonList("device_4412_day_tags"),
        Collections.singletonList("dbo_device_4412_day")
    );

    IntStream.range(0, 10).forEach(index -> {
      List<QueryResult> result = QueryExecutionJob.builder()
          .queriesNumber(100)
          .executorService(executorService)
          .connectionProvider(threadLocalConnectionProvider)
          .randomQueryProvider(randomQueryProvider)
          .build()
          .run();
      handleResult(result);
    });

    executorService.shutdown();
  }

  @SneakyThrows
  private static void handleResult(List<QueryResult> result) {
    long maxQueryDuration = result.stream()
        .map(QueryResult::durationMs)
        .max(Comparator.naturalOrder())
        .orElse(0L);
    long minQueryDuration = result.stream().map(QueryResult::durationMs)
        .min(Comparator.naturalOrder())
        .orElse(0L);
    Double avgQueryDuration = result.stream()
        .collect(Collectors.averagingDouble(QueryResult::durationMs));
    long successNumber = result.stream()
        .filter(QueryResult::isSuccess)
        .count();

    final BufferedWriter bufferedWriter = Files
        .newBufferedWriter(Paths.get("results/result.txt"), StandardOpenOption.APPEND);
    bufferedWriter.append("maxQueryDuration = " + maxQueryDuration);
    bufferedWriter.newLine();
    bufferedWriter.append("minQueryDuration = " + minQueryDuration);
    bufferedWriter.newLine();
    bufferedWriter.append("avgQueryDuration = " + avgQueryDuration);
    bufferedWriter.newLine();
    bufferedWriter.append("successNumber = " + successNumber);
    bufferedWriter.newLine();
    bufferedWriter.newLine();
    bufferedWriter.close();
  }

}

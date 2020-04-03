package com.example.azuresynapse.queryexecution;

import lombok.SneakyThrows;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

abstract class AbstractRandomQuery {

    private final List<String> tags;
    private final List<String> aggFunctions;
    private final List<String> dayPartitions;

    AbstractRandomQuery() {
        this.tags = readTags("src/main/resources/tags.txt");
        this.dayPartitions = readDayPartitions("src/main/resources/day_partitions.txt");
        this.aggFunctions = Arrays.asList(
                "count(%s)",
                "min(%s)",
                "max(%s)",
                "avg(TRY_CAST(%s AS float))"
        );
    }

    @SneakyThrows
    private static List<String> readTags(String path) {
        return Files.readAllLines(Paths.get(path));
    }

    @SneakyThrows
    private static List<String> readDayPartitions(String path) {
        return Files.readAllLines(Paths.get(path));
    }

    String randomTag() {
        return RandomUtils.randomElement(tags);
    }

    String randomDayPartition() {
        return RandomUtils.randomElement(dayPartitions);
    }

    String randomAggFunction() {
        return RandomUtils.randomElement(aggFunctions);
    }

}

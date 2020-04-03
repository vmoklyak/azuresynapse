package com.example.azuresynapse.queryexecution;

import lombok.SneakyThrows;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class RandomTagQuery extends AbstractRandomQuery {

    private final List<String> dayTables;
    private final String pivotQueryTemplate;

    RandomTagQuery(List<String> dayTables) {
        super();
        this.dayTables = dayTables;
        this.pivotQueryTemplate = readPivotQueryTemplate("src/main/resources/pivot_query_template.txt");
    }

    String randomAggTagQuery() {
        return String.format("SELECT %s, '%s' AS random_str FROM [dbo].[%s] WHERE tag = '%s'",
                String.format(randomAggFunction(), "tag_value"), new Random().nextInt(), randomDayTable(), randomTag());
    }

    String randomDayTagQuery() {
        return String.format("SELECT %s, '%s' AS random_str FROM [dbo].[%s] WHERE tag = '%s' AND date_partition = %s",
                String.format(randomAggFunction(), "tag_value"), new Random().nextInt(), randomDayTable(), randomTag(), randomDayPartition());
    }

    String randomDayGroupTagQuery() {
        return String.format("Select tag_value, %s AS random_str FROM [dbo].[%s] WHERE tag = '%s' AND date_partition = %s GROUP BY tag_value",
                new Random().nextInt(), randomDayTable(), randomTag(), randomDayPartition());
    }

    String randomAggOverGroupTagQuery() {
        return String.format("SELECT max(LEN(tag_value) * count) FROM (Select tag_value, count(*) AS count, %s AS random_str FROM [dbo].[%s] WHERE tag = '%s' GROUP BY tag_value) AS tbl",
                new Random().nextInt(), randomDayTable(), randomTag());
    }

    String randomDayAggOverGroupTagQuery() {
        return String.format("SELECT max(LEN(tag_value) * count) FROM (Select tag_value, count(*) AS count, %s AS random_str FROM [dbo].[%s] WHERE tag = '%s' AND date_partition = %s GROUP BY tag_value) AS tbl",
                new Random().nextInt(), randomDayTable(), randomTag(), randomDayPartition());
    }

    String randomDayPivotTagQuery() {
        String tagNames = IntStream.range(0, RandomUtils.randomPositiveInt(10))
                .mapToObj(index -> String.format("[%s]", randomTag()))
                .distinct()
                .collect(Collectors.joining(","));
        return String.format(pivotQueryTemplate,
                tagNames, randomDayTable(), randomDayPartition(), tagNames);
    }

    @SneakyThrows
    private static String readPivotQueryTemplate(String path) {
        return String.join(System.lineSeparator(), Files.readAllLines(Paths.get(path)));
    }

    private String randomDayTable() {
        return RandomUtils.randomElement(dayTables);
    }

}

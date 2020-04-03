package com.example.azuresynapse.queryexecution;

import java.util.List;
import java.util.Random;

class RandomColumnQuery extends AbstractRandomQuery {

    private final List<String> dayTables;

    RandomColumnQuery(List<String> dayTables) {
        super();
        this.dayTables = dayTables;
    }

    String randomAggQuery() {
        return String.format("SELECT %s, '%s' AS random_str FROM [dbo].[%s]",
                String.format(randomAggFunction(), randomTag()), new Random().nextInt(), randomDayTable());
    }

    String randomDayAggQuery() {
        return String.format("SELECT %s, '%s' AS random_str FROM [dbo].[%s] WHERE date_partition = %s",
                String.format(randomAggFunction(), randomTag()), new Random().nextInt(), randomDayTable(), randomDayPartition());
    }

    String randomAggOverGrouping() {
        String column = randomTag();
        return String.format("SELECT max(LEN(%s) * count) FROM (Select %s, count(*) as count, %s AS random_str FROM [dbo].[%s] GROUP BY %s) AS tbl",
                column, column, new Random().nextInt(), randomDayTable(), column);
    }

    String randomDayAggOverGrouping() {
        String column = randomTag();
        return String.format("SELECT max(LEN(%s) * count) FROM (Select %s, count(*) as count, %s AS random_str FROM [dbo].[%s] WHERE date_partition = %s GROUP BY %s) AS tbl",
                column, column, new Random().nextInt(), randomDayTable(), randomDayPartition(), column);
    }

    String randomDayGroupQuery() {
        String column = randomTag();
        return String.format("Select %s, %s AS random_str FROM [dbo].[%s] WHERE date_partition = %s GROUP BY %s",
                column, new Random().nextInt(), randomDayTable(), randomDayPartition(), column);
    }

    private String randomDayTable() {
        return RandomUtils.randomElement(dayTables);
    }

}

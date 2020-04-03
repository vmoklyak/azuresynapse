package com.example.azuresynapse.queryexecution;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

class RandomQueryProvider {

    private List<String> tagDayTables;
    private List<String> columnDayTables;
    private Supplier<Supplier<String>> randomQuerySupplier;

    RandomQueryProvider(List<String> tagDayTables, List<String> columnDayTables) {
        this.tagDayTables = tagDayTables;
        this.columnDayTables = columnDayTables;

        randomQuerySupplier = init();
    }

    private Supplier<Supplier<String>> init() {
        RandomTagQuery randomTagQuery = new RandomTagQuery(tagDayTables);
        RandomColumnQuery randomColumnQuery = new RandomColumnQuery(columnDayTables);

        List<Supplier<String>> querySuppliers = new ArrayList<>();
        if (!tagDayTables.isEmpty()) {
            querySuppliers.add(randomTagQuery::randomDayTagQuery);
            querySuppliers.add(randomTagQuery::randomDayAggOverGroupTagQuery);
            querySuppliers.add(randomTagQuery::randomDayGroupTagQuery);
            querySuppliers.add(randomTagQuery::randomDayPivotTagQuery);

            querySuppliers.add(randomTagQuery::randomAggTagQuery);
            querySuppliers.add(randomTagQuery::randomAggOverGroupTagQuery);
        }

        if (!columnDayTables.isEmpty()) {
            querySuppliers.add(randomColumnQuery::randomDayAggQuery);
            querySuppliers.add(randomColumnQuery::randomDayAggOverGrouping);
            querySuppliers.add(randomColumnQuery::randomDayGroupQuery);

            querySuppliers.add(randomColumnQuery::randomAggQuery);
            querySuppliers.add(randomColumnQuery::randomAggOverGrouping);
        }

        return () -> RandomUtils.randomElement(querySuppliers);
    }

    String provide() {
        return randomQuerySupplier.get().get();
    }

}

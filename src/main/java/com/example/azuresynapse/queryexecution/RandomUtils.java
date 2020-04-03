package com.example.azuresynapse.queryexecution;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Random;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class RandomUtils {

    static int randomPositiveInt(int bound) {
        return 1 + new Random().nextInt(bound - 1);
    }

    static <T> T randomElement(List<T> list) {
        return list.get(new Random().nextInt(list.size()));
    }

}


package com.example.azuresynapse.queryexecution;

import javafx.util.Pair;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;

@Slf4j
class ThreadLocalConnectionProvider implements ConnectionProvider {

    private final ThreadLocal<Connection> connectionThreadLocal;

    @SafeVarargs
    ThreadLocalConnectionProvider(String dbUrl, Pair<String, String>... loginPasswords) {
        List<Pair<String, String>> loginPasswordList = Arrays.asList(loginPasswords);
        connectionThreadLocal = ThreadLocal.withInitial(() -> {
            Pair<String, String> randomLoginPassword = RandomUtils.randomElement(loginPasswordList);
            log.info("Create connection: {} - {}", randomLoginPassword.getKey(), randomLoginPassword.getValue());
            return connection(dbUrl, randomLoginPassword.getKey(), randomLoginPassword.getValue());
        });
    }

    @Override
    public Connection provide() {
        return connectionThreadLocal.get();
    }

    @SneakyThrows
    private static Connection connection(String dbUrl, String user, String password) {
        return DriverManager.getConnection(dbUrl, user, password);
    }

}

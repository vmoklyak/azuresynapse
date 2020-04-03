package com.example.azuresynapse.queryexecution;

import java.sql.Connection;

public interface ConnectionProvider {

    Connection provide();

}

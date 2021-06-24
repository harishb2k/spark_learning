package org.example;

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap$;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import scala.Tuple2;
import scala.collection.immutable.Map$;

import java.sql.Connection;

public class UsingSparkJdbcUtils {

    public static void main(String[] args) {
        // Read database, table name to check and password from env var
        String database = System.getenv("database");
        String table = System.getenv("table");
        String password = System.getenv("password");

        // Setup database porperties
        CaseInsensitiveMap<String> properties = CaseInsensitiveMap$.MODULE$.apply(Map$.MODULE$.empty())
                .$plus(new Tuple2<>("driver", "com.mysql.jdbc.Driver"))
                .$plus(new Tuple2<>("url", "jdbc:mysql://localhost:3306/" + database))
                .$plus(new Tuple2<>("user", "root"))
                .$plus(new Tuple2<>("password", password))
                .$plus(new Tuple2<>("dbtable", table));

        // Get connection and use JdbcUtils to check if table exists or not
        JdbcOptionsInWrite options = new JdbcOptionsInWrite(properties);
        Connection conn = JdbcUtils.createConnectionFactory(options).apply();
        boolean exists = JdbcUtils.tableExists(conn, options);

        System.out.println("Table exists check: " + exists);
    }
}

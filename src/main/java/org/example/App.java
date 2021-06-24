package org.example;


import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap$;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import scala.Tuple2;
import scala.collection.immutable.HashSet;
import scala.collection.immutable.HashSet$;
import scala.collection.immutable.Map$;

import java.sql.Connection;


/**
 * Hello world!
 */
public class App {

    public static void readFile() {
        String logFile = "addresses.csv";

        SparkSession s = SparkSession.builder()
                .master("local")
                .getOrCreate();

        Dataset<String> logData = s.read().textFile(logFile);
        long count = logData.filter(new FilterFunction<String>() {
            @Override
            public boolean call(String value) throws Exception {
                return value.contains("91234");
            }
        }).count();
        System.out.println("--> " + count);
    }

    public static void readMySql() {
        SparkSession s = SparkSession.builder()
                .master("local")
                .getOrCreate();
        s.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/hyperdata_control_plane")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("mytable", "my_table")
                .option("user", "root")
                .option("password", "root")
                .option("dbtable", "pipelines")
                .load()
                .foreach((ForeachFunction<Row>) row -> {
                    System.out.println(row.getString(1));
                });
    }

    public static void main(String[] args) {
        CaseInsensitiveMap<String> c = CaseInsensitiveMap$.MODULE$.apply(Map$.MODULE$.empty());
        c = c.$plus(new Tuple2<>("driver", "com.mysql.jdbc.Driver"));
        c = c.$plus(new Tuple2<>("url", "jdbc:mysql://localhost:3306/hyperdata_control_plane"));
        c = c.$plus(new Tuple2<>("user", "root"));
        c = c.$plus(new Tuple2<>("password", "root"));
        c = c.$plus(new Tuple2<>("dbtable", "pipelines1"));
        System.out.println(c);

        JdbcOptionsInWrite options = new JdbcOptionsInWrite(c);
        Connection conn = JdbcUtils.createConnectionFactory(options).apply();
        System.out.println(conn);

        boolean exists = JdbcUtils.tableExists(conn, options);
        System.out.println(exists);
        // readMySql();
    }
}

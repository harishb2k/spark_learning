package org.example;


import org.apache.spark.Partition;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap$;
import org.apache.spark.sql.execution.datasources.LogicalRelation$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCPartition$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation$;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.immutable.Map$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;


/**
 * Hello world!
 */
public class App {
    // Read database, table name to check and password from env var
    static String database = System.getenv("database");
    static String table = System.getenv("table");
    static String password = System.getenv("password");


    public static void custom() {

        // Setup database porperties
        CaseInsensitiveMap<String> properties = CaseInsensitiveMap$.MODULE$.apply(Map$.MODULE$.empty())
                .$plus(new Tuple2<>("driver", "com.mysql.jdbc.Driver"))
                .$plus(new Tuple2<>("url", "jdbc:mysql://localhost:3306/" + database))
                .$plus(new Tuple2<>("user", "root"))
                .$plus(new Tuple2<>("password", "root"))
                .$plus(new Tuple2<>("dbtable", table));

        // Get connection and use JdbcUtils to check if table exists or not
        JdbcOptionsInWrite options = new JdbcOptionsInWrite(properties);

        StructType struct = new StructType().add("id", "int").add("name", "string");

        SparkSession s = SparkSession.builder()
                .master("local")
                .getOrCreate();

        Partition partition = JDBCPartition$.MODULE$.apply(null, 0);
        Partition partitions[] = new Partition[1];
        partitions[0] = partition;

        JDBCRelation jdbcRelation = JDBCRelation$.MODULE$.apply(struct, partitions, options, s);
        System.out.println(jdbcRelation);
        RDD<Row> rows = jdbcRelation.buildScan(new String[]{"name"}, new Filter[]{});
        System.out.println(rows);
        long count = rows.count();
        System.out.println("Count = " + count);

        // rows.foreach(new X());


        Dataset.ofRows(s, LogicalRelation$.MODULE$.apply(jdbcRelation, false)).foreach(row -> {
            System.out.println(row);
        });
    }

    public static class X extends AbstractFunction1<Row, BoxedUnit> implements Serializable {
        @Override
        public BoxedUnit apply(Row v1) {
            System.out.println(v1.getString(1));
            return null;
        }
    }

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
        SparkSession session = SparkSession.builder().master("local").getOrCreate();
        session.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/" + database)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("mytable", table)
                .option("user", "root")
                .option("password", password)
                .option("dbtable", table)
                .load()
                .foreach((ForeachFunction<Row>) row -> {
                    System.out.println(row.getString(1));
                });
    }

    private static Connection getRemoteConnection() throws Exception {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String dbName = System.getenv("RDS_DB_NAME");
            String userName = System.getenv("RDS_USERNAME");
            String password = System.getenv("RDS_PASSWORD");
            String hostname = "";
            String port = System.getenv("RDS_PORT");
            String jdbcUrl = "jdbc:mysql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + password;
            System.out.println("Getting remote connection with connection string from environment variables.");
            Connection con = DriverManager.getConnection(jdbcUrl);
            System.out.println("Remote connection successful.");
            return con;
        } catch (ClassNotFoundException e) {
            System.out.println(e.toString());
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        if (true) {
            custom();
            // readMySql();
            return;
        }

        // TableIdentifier ti = CatalystSqlParser$.MODULE$.parseTableIdentifier("select * from table1");
        // System.out.println(ti);

        Expression e = CatalystSqlParser$.MODULE$.parseExpression("select * from table1");

        LogicalPlan p = CatalystSqlParser$.MODULE$.parsePlan("select a, b from table1");
        System.out.println(e);
        System.out.println(p);


        StructType struct = new StructType().add("a", "int").add("b", "string");
        System.out.println(struct);


        p.resolve(struct, new AbstractFunction2<String, String, Object>() {
            @Override
            public Object apply(String v1, String v2) {
                return v2;
            }
        });
    }
}

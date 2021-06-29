package org.example;

import org.apache.spark.Partition;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

/**
 * This example shows how Spark internally works for JDBC. <br>
 * <pre>
 * 1. When JDBC is provided in format() - then it will create a BaseRelation (of type JDBCRelation)
 * 2. It will run "select 1" to find table schema
 *    - Here we injected the schema by hand
 * 3. Create a data frame
 *    - Here we created it by hand
 *
 * Now once we have data frame then we can just use normal spark code to print or chain data frame
 *
 *
 * How to run: set following env variable
 * database, table, password (we assume the user name is root - you can change the code)
 */
public class DirectUseOfJdbcRelation {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();

        // Read database, table name to check and password from env var
        String database = System.getenv("database");
        String table = System.getenv("table");
        String password = System.getenv("password");

        // Setup database properties
        CaseInsensitiveMap<String> properties = CaseInsensitiveMap$.MODULE$.apply(Map$.MODULE$.empty())
                .$plus(new Tuple2<>("driver", "com.mysql.jdbc.Driver"))
                .$plus(new Tuple2<>("url", "jdbc:mysql://localhost:3306/" + database))
                .$plus(new Tuple2<>("user", "root"))
                .$plus(new Tuple2<>("password", password))
                .$plus(new Tuple2<>("dbtable", table));

        // Get connection and use JdbcUtils to check if table exists or not
        JdbcOptionsInWrite options = new JdbcOptionsInWrite(properties);

        // For now we do not consider DB partition
        Partition partition = JDBCPartition$.MODULE$.apply(null, 0);
        Partition[] partitions = new Partition[1];
        partitions[0] = partition;

        // =============================================================================================================
        // Step 1 - Spark would make DB connection and will find the table schema - here we passed the schema by hand
        // Define database scheme by hand
        StructType struct = new StructType().add("id", "int").add("name", "string");

        // =============================================================================================================
        // Step 2 - Spark would make a object of JDBCRelation
        // Let's build the JDBC session
        JDBCRelation jdbcRelation = JDBCRelation$.MODULE$.apply(struct, partitions, options, sparkSession);

        // =============================================================================================================
        // Step 3.a - You can make a RDD if you want
        // A simple way to scan the table and get row counts
        RDD<Row> rows = jdbcRelation.buildScan(new String[]{"name"}, new Filter[]{});
        long count = rows.count();
        System.out.println("Count = " + count);

        // =============================================================================================================
        // Step 3.a - Spark makes a data frame using following codee
        // Let's create the DataFrame by hand
        Dataset.ofRows(sparkSession, LogicalRelation$.MODULE$.apply(jdbcRelation, false)).foreach(row -> {
            System.out.println(row);
        });
    }
}

package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

import java.util.Locale

object RunJob extends App {

  def withOrigin[T](ctx: String)(f: => T): T = {
    f
  }

  def r(s: String): String = withOrigin(s) {
    "harish"
  }

  def _main(args: Array[String]): Unit = {
    r("bohara")
  }


  def main(args: Array[String]) {

    val logFile = "/Users/harishbohara/workspace/personal/spark_learning/src/test/java/org/example/AppTest.java"


    import scala.collection.mutable
    val parserBuilders = mutable.Buffer.empty[String]

    val initial = mutable.Buffer.empty[String]
    initial += "harish"
    var i = 0
    var x = parserBuilders.foldLeft("Harish") { (parser, builder) =>
      builder
    }
    println(x)


    /*val lexer = new SqlBaseLexer(CharStreams.fromString("select * from name"))

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    parser.addParseListener(new ParseTreeListener() {
      override def visitTerminal(node: TerminalNode): Unit = {
        println(node)
      }

      override def visitErrorNode(node: ErrorNode): Unit = {
        println(node)
      }

      override def enterEveryRule(ctx: ParserRuleContext): Unit = {
        println(ctx)
      }

      override def exitEveryRule(ctx: ParserRuleContext): Unit = {
        println(ctx)
      }
    })
    parser.singleTableSchema()*/


    val scc = new SparkConf();
    scc.setMaster("local")
    scc.setAppName("a")
    /*
    val sparkSession: SparkSession = SparkSession.builder()
      .config(scc)
      .getOrCreate()*/

    // Example 1 - adding extension to spark
    type ExtensionBuilder = SparkSessionExtensions => Unit
    // val f: ExtensionBuilder = { e => e.injectOptimizerRule(GP) }
    // val f: ExtensionBuilder = { e => e.injectCheckRule(GP) }


    val s = SparkSession.builder()
      .master("local")

      .withExtensions { extensions =>
        // extensions.injectOptimizerRule(GP)
        extensions.injectResolutionRule(GP)

        // Example 1 - adding extension to spark
        /*extensions.injectResolutionRule { session =>
          println("Got injectResolutionRule")

          return
        }*/
        /* extensions.injectParser { (session, parser) =>
           println("Got injectParser")

           return
         }*/
        /*extensions.injectCheckRule( (session) => {
          println("Got here")
          return
        })*/

      }
      .getOrCreate()
    //self.spark.sparkContext.setLogLevel(log_level)
    s.sparkContext.setLogLevel("TRACE")


    // val sparkContext = sparkSession.sparkContext


    // Example 2 - How can you use the Spark SQL parser and create a LogicalPlan
    /*
    val logData = s.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    val spark = s
    var pp = spark.sessionState.sqlParser.parsePlan("select * from table1, table2 where table1.id=table2.id and table1.name='harish'")
    */

    val jdbcDF = s.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/my_db")
      .option("dbtable", "my_table")
      .option("user", "root")
      .option("password", "root")
      .load()


    jdbcDF.createOrReplaceTempView("names")
    jdbcDF.sqlContext.sql("select /*+ BROADCAST(names) */ * from names where id=1").collect.foreach(println)
    // jdbcDF.sqlContext.sql("select /*+ HARISH(names) */ * from names where id=1").collect.foreach(println)
    // jdbcDF.sqlContext.sql("select * from names where id=1").collect.foreach(println)


    // Example 3 - How to create a logical plan and get the query plan
    /*
    val conf = new SQLConf()
    s.sparkContext.getConf.getAll.foreach { case (k, v) =>
      conf.setConfString(k, v)
    }
    var sessionStateBuilder = new SessionStateBuilder(s)
    var sessionState = sessionStateBuilder.build();
    var sparkSqlParser = new SparkSqlParser(conf)
     var lp = sparkSqlParser.parsePlan("select id, name from my_table where id=1")
    println(lp)
    val struct = new StructType().add("id", "int").add("name", "string")
    lp.resolve(struct, sessionState.analyzer.resolver);
    var pp = sessionStateBuilder.build().planner.plan(lp)
    println(pp)
     */


    // Example 4 - How to create a logical plan from a DB table
    /*
    var m = CaseInsensitiveMap[String](Map.empty)
     m += (("url" -> "jdbc:mysql://localhost:3306/my_db"))
     m += (("dbtable" -> "my_table"))
     m += (("user" -> "root"))
     m += (("password" -> "root"))

     var relation = DataSource.apply(
       s,
       paths = null,
       userSpecifiedSchema = None,
       className = "jdbc",
       options = m
     ).resolveRelation()
     var si = relation.sizeInBytes;
     var lr = LogicalRelation(relation);
  */

    s.stop()

  }

  def resolveExpressions(r: PartialFunction[Expression, Expression]): String = {
    resolveExpressions(r)
    ""
  }
}

case class GP(session: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case h: UnresolvedHint if "HARISH".contains(h.name.toUpperCase(Locale.ROOT)) =>
      h.child
  }
}

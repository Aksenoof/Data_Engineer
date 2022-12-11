import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Main extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

//  val lines = spark.sparkContext.parallelize(
//    Seq("Spark Intellij Idea Scala test one",
//        "Spark Intellij Idea Scala test two",
//        "Spark Intellij Idea Scala test three"))
//
//  val counts = lines
//    .flatMap(line => line.split(" "))
//    .map(word => (word, 1))
//    .reduceByKey(_ + _)
  val jdbcDF = spark.read
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/postgres")
                .option("dbtable", "public.departments")
                .option("user", "postgres")
                .option("password", "postgres")
                .load()

  var newTable = jdbcDF.filter("number_employees > 2")
  newTable = newTable.withColumn("auto", when(col("number_employees") === lit(6),true).otherwise(false))

  newTable.write
          .format("jdbc")
          .mode(SaveMode.Overwrite)
          .option("url", "jdbc:postgresql://localhost:5432/postgres")
          .option("dbtable", "public.employees")
          .option("user", "postgres")
          .option("password", "postgres")
          .save()

  println(jdbcDF.show(5))

  spark.sparkContext.setLogLevel("WARN")

//  counts.foreach(println)
}

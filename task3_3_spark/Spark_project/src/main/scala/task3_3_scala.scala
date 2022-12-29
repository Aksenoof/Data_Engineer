
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object task3_3_scala extends App {

  // настройка сессии спарк
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark_task3_3")
    .getOrCreate()

  // подключаемся к посгрес для чтения таблицы public.data_lk
  val data_lk = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/postgres")
    .option("dbTable", "public.data_lk")
    .option("user", "postgres")
    .option("password", "postgres")
    .load()

  // подключаемся к посгрес для чтения таблицы public.data_web
  val data_web = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/postgres")
    .option("dbTable", "public.data_web")
    .option("user", "postgres")
    .option("password", "postgres")
    .load()

  //Id посетителя, Возраст посетителя
  var newTable = data_lk.withColumn("age", round(datediff(current_date, to_date(col("dob"),"yyyy-mm-dd"))/ 365).cast("int"))

  //Пол посетителя (постарайтесь описать логику вычисления пола в отдельной пользовательской функции)
  val calc_gender = udf((args: String) => {
    val name = args.split(' ')
    val surname: String = name(0)
    val middleName: String = name(2)
    if (((surname.takeRight(2) == "ов") || (surname.takeRight(2) == "ев")) && ((middleName.takeRight(2) == "ич") || (middleName.takeRight(2) == "ыч"))) "M"
    else "W"
  })

  newTable = newTable.withColumn("gender", calc_gender(col("fio")))
    .drop("id_lk")

  //Любимая тематика новостей
  var newTable1 = data_web.groupBy("user_id")
    .agg(max("tag").as("favoriteTopic"))

  //Любимый временной диапазон посещений
  var newTable2 = data_web.select(col("user_id"), from_unixtime(col("timestamp")).as("eventTime"))
    .withColumn("range", floor(hour(col("eventTime")) / 4))
    .withColumn("timeRange", expr("case when range = 0 then '0 - 4'" + "when range = 1 then '4 - 8'" +
                                                                                        "when range = 2 then '8 - 12'" +
                                                                                        "when range = 3 then '12 - 16'" +
                                                                                        "when range = 4 then '16 - 20'" +
                                                                                        "when range = 5 then '20 - 24'" +
                                                                                        "else 'Unknown' end"))
    .groupBy("user_id")
    .agg(max("timeRange").as("favoriteTime"))

  //Id личного кабинета, разница в днях между созданием ЛК и датой последнего посещения. (-1 если ЛК нет)
  var newTable3 = data_web.select(col("user_id"), col("sign"), from_unixtime(col("timestamp")).as("eventTime"))
    .groupBy( "user_id", "sign")
    .agg(max("eventTime"))
  newTable3 = data_lk.as("d_lk").join(newTable3.as("nT3"), col("d_lk.id") ===  col("nT3.user_id"),"left")
    .withColumn("dateDiff", when(col("nT3.sign") === true, datediff(col("nT3.max(eventTime)"), col("d_lk.doc"))).otherwise(-1))
    .drop("id","fio", "dob", "doc", "sign", "max(eventTime)")

  //Общее кол-во посещений сайта
  var newTable4 = data_web.filter(data_web("actions") === "visit")
    .groupBy( "user_id")
    .agg(count("*").as("countVisit"))

  //Средняя длина сессии(сессией считаем временной промежуток, который охватывает последовательность событий,
  // которые происходили подряд с разницей не более 5 минут)
  val userWindow = Window.partitionBy("user_id").orderBy("eventTime")
  val prevActive = lag("eventTime", 1).over(userWindow)
  var newTable5 = data_web.select(col("user_id"), col("actions"), from_unixtime(col("timestamp")).as("eventTime"))
  newTable5 = newTable5.withColumn("lag", when(prevActive.isNull, 0).otherwise(prevActive))
    .withColumn("minEventSec", (minute(col("eventTime")) * 60) + second(col("eventTime")))
    .withColumn("lagSec", minute(col("lag")) * 60 + second(col("lag")))
    .withColumn("diffTime", when(abs(col("minEventSec") - col("lagSec")).isNull, 0).otherwise(abs(col("minEventSec") - col("lagSec"))))
    .withColumn("Session", when(col("diffTime") === 0 || col("diffTime") > 300, 1).otherwise(0))
    .withColumn("sumTimeSessions", sum(col("diffTime")).over(Window.partitionBy("user_id")))
    .withColumn("countSession", sum(col("Session")).over(Window.partitionBy("user_id")))
    .withColumn("meanSession_sec", floor(col("sumTimeSessions") / col("countSession")))
  var newTable6 = newTable5.groupBy("user_id","meanSession_sec")
    .count()
    .select("user_id","meanSession_sec")

  //Среднее кол-во активностей в рамках одной сессии
  var newTable7 = newTable5.withColumn("countActions", count(col("actions")).over(Window.partitionBy("user_id")))
    .withColumn("meanCountActions", floor(col("countActions")/col("countSession")))
    .groupBy("user_id", "meanCountActions").count()
    .select("user_id", "meanCountActions")

  //сбор витрины
  newTable = newTable.as("nT").join(newTable1.as("nT1"), col("nT.id") ===  col("nT1.user_id"),"left")
    .join(newTable2.as("nT2"), col("nT.id") ===  col("nT2.user_id"),"left")
    .join(newTable3.as("nT3"), col("nT.id") ===  col("nT3.user_id"),"left")
    .join(newTable4.as("nT4"), col("nT.id") ===  col("nT4.user_id"),"left")
    .join(newTable6.as("nT6"), col("nT.id") ===  col("nT6.user_id"),"left")
    .join(newTable7.as("nT7"), col("nT.id") ===  col("nT7.user_id"),"left")
    .orderBy("id")
    .drop( "fio", "dob", "doc", "user_id")

  //вычислить целевую аудиторию для данных новостей
  //критерии:1-наличие личного кабинета, 2-общее кол-во посещений сайта, 3-средняя длина сессии, 4-средняя активность
  var newTable8 = newTable.filter(when(col("dateDiff") > 0, true))
    .sort(desc("meanCountActions"))
    .sort(desc("meanSession_sec"))
    .sort(desc("countVisit"))
  newTable8 = newTable8.limit((newTable8.count() * 0.5).toInt) //взял 50% т.к мало данных


  println(newTable.show())
  println(newTable8.show())

  spark.sparkContext.setLogLevel("WARN")
}

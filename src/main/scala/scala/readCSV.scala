package scala

/**
 * @author Alexander Letyagin
 *         Тестовое задание по scala/spark:
 *         Написать spark приложение, которое в локальном режиме выполняет следующее:
 *         По имеющимся данным о рейтингах книг посчитать агрегированную статистику по ним.
 *         1. Прочитать csv файл: book.csv
 *            2. Вывести схему для dataframe полученного из п.1
 *            3. Вывести количество записей
 *            4. Вывести информацию по книгам у которых рейтинг выше 4.50
 *            5. Вывести средний рейтинг для всех книг.
 *            6. Вывести агрегированную инфорацию по количеству книг в диапазонах:
 *            0 - 1
 *            1 - 2
 *            2 - 3
 *            3 - 4
 *            4 - 5
 */


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object readCSV extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("spark-test1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // 1. Прочитать csv файл: book.csv
  val df = spark.read
    .option("header", "true")
    .option("delimiter", ",")
    .option("multiLine", "true")
    .option("ignoreLeadingWhiteSpace", "true")
    .option("ignoreTrailingWhiteSpace", "true")
    .option("inferSchema", "true")
    .option("mode", "DROPMALFORMED")
    .option("quote", "")
    //.option("QuoteMode", "NONE")
    .option("escape", "\\")
    .csv("src/main/resources/books.csv")

  // 2. Вывести схему для dataframe полученного из п.1
  println("2. Dataframe schema of books.csv")
  df.printSchema()

  // 2. Вывести количество записей
  // filter используется для отсева записей с большим к-вом строк (больше ,)
  println("3. Record in books: " + df.filter(floor("average_rating").isNotNull).count())

  //4. Вывести информацию по книгам у которых рейтинг выше 4.50
  println("4. Show more than 4.5 rated books")
  println(df.filter("average_rating > 4.5").show(false))

  // 5. Вывести средний рейтинг для всех книг.
  println("5. Average book rating: " + df.select(avg("average_rating")).collect()(0)(0))

  //  6. Вывести агрегированную инфорацию по количеству книг в диапазонах:
  //    0 - 1
  //    1 - 2
  //    2 - 3
  //    3 - 4
  //    4 - 5
  println("6. Aggregated rating base count")
  df
    .filter("average_rating >= 0.0")
    .groupBy(floor("average_rating").as("rating"))
    .count()
    .sort("rating")
    .show(false)

}
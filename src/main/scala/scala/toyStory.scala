package scala

/**
 * @author Alexander Letyagin
 *
 *         ## Задача
 *         Написать spark приложение, которое в локальном режиме выполняет следующее:
 *         По имеющимся данным о рейтингах фильмов (MovieLens: 100 000 рейтингов) посчитать агрегированную статистику по ним.
 *
 *         ## Описание данных
 *         Имеются следующие входные данные:
 *         Архив с рейтингами фильмов.
 *         Файл README содержит описания файлов.
 *         Файл u.data содержит все оценки, а файл u.item — список всех фильмов. (используются только эти два файла)
 *         id_film=32
 *
 *         1. Прочитать данные файлы.
 *            2. создать выходной файл в формате json, где
 *            Поле `“Toy Story ”` нужно заменить на название фильма, соответствующего id_film и указать для заданного фильма количество поставленных оценок в следующем порядке: `"1", "2", "3", "4", "5"`. То есть сколько было единичек, двоек, троек и т.д.
 *            В поле `“hist_all”` нужно указать то же самое только для всех фильмов общее количество поставленных оценок в том же порядке: `"1", "2", "3", "4", "5"`.
 *
 *         Пример решения:
 *
 *         {
 *         "Toy Story": [
 *         134,
 *         123,
 *         782,
 *         356,
 *         148
 *         ],
 *         "hist_all": [
 *         134,
 *         123,
 *         782,
 *         356,
 *         148
 *         ]
 *         }
 *
 * */


import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

object toyStory extends App {

  // TODO: стоит вынести в Args
  var movieId = 32

  val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("spark-test1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Читаем файл с фактом оценок
  case class Ratings(
                      userId: Int,
                      movieId: Int,
                      rating: Int,
                      timestamp: Int
                    )

  var usersSchema = Encoders.product[Ratings].schema
  val users = spark.read
    .option("header", "false")
    .option("delimiter", "\t")
    .option("ignoreLeadingWhiteSpace", "true")
    .option("inferSchema", "true")
    .schema(usersSchema)
    .csv("src/main/resources/u.data")

  // Читаем файл с фильмами
  case class Movies(
                     movieId: Int,
                     movieTitle: String,
                     releaseDate: Date,
                     videoReleaseData: Date,
                     imdbUrl: String,
                     unknown: Int,
                     action: Int,
                     adventure: Int,
                     animation: Int,
                     childrens: Int,
                     comedy: Int,
                     crime: Int,
                     documentary: Int,
                     drama: Int,
                     fantasy: Int,
                     filmnoir: Int,
                     horror: Int,
                     musical: Int,
                     mystery: Int,
                     romance: Int,
                     scifi: Int,
                     thriller: Int,
                     war: Int,
                     western: Int
                   )

  var itemsSchema = Encoders.product[Movies].schema
  val items = spark.read
    .option("header", "false")
    .option("delimiter", "|")
    .option("ignoreLeadingWhiteSpace", "true")
    .option("inferSchema", "true")
    .schema(itemsSchema)
    // TODO сделать нормальную обработку пути/имени, cli argument?
    .csv("src/main/resources/u.item")

  // Агрегируем оценки по всем фильмам
  val allMovies = users.groupBy("rating")
    .agg(count("rating").alias("count"))
    .orderBy(asc("rating"))

  // Агрегируем оценки по искомому фильму
  val selectedMovie = users
    .filter(col("movieId") === lit(movieId))
    .groupBy("rating")
    .agg(count("rating").alias("count"))
    .orderBy(asc("rating"))

  // Получаем наименование искомого фильма
  val movieName = items.select("movieTitle")
    .filter(col("movieId") === lit(movieId))
    .collect()(0)(0).toString

  import spark.implicits._

  // формируем df для конвертации в json заданной структуры
  // конвертируя оценки из колонки в массив
  val res = Seq(
    (selectedMovie.select("count")
      .collect().map(_.getLong(0))
      , allMovies.select("count")
      .collect().map(_.getLong(0))
    )
  ).toDF(movieName, "hist_all")

  // конвертируем результирующий df в json строку
  val resStr = res.toJSON.collect().mkString(", ")
  // формируем из строки rdd и сохраняем его в файл
  // df.write.csv сохраняет в json построчно без общего { строка, строка }
  // можно, наверное, и сделать постобработку файла, пока не нашел другого способа
  // получить требуемый в задаче результат.
  val resRDD = spark.sparkContext.parallelize(Seq(resStr))
  resRDD.saveAsTextFile("target/out_json")

}

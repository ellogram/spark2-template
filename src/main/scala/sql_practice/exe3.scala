package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object exe3 {
  def exec3(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")

    println()
    println("1. How many unique levels of difficulty?")
    toursDF
      .select($"tourDifficulty")
      .distinct()
      .show()

    println("2. What is the min/max/avg of tour prices?")
    toursDF
      .select($"tourPrice")
      .agg(min($"tourPrice").as("min"), max($"tourPrice").as("max"), avg($"tourPrice").as("avg"))
      .withColumn("avg", regexp_replace(format_number($"avg", 1), ",", ""))
      .show()

    println("3. What is the min/max/avg of price for each level of difficulty?")
    toursDF
      .select($"tourDifficulty", $"tourPrice")
      .groupBy($"tourDifficulty")
      .agg(min($"tourPrice").as("min"), max($"tourPrice").as("max"), avg($"tourPrice").as("avg"))
      .orderBy($"avg".desc)
      .withColumn("avg", regexp_replace(format_number($"avg", 1), ",", ""))
      .show()

    println("4. What is the min/max/avg of price and min/max/avg of duration for each level of difficulty?")
    toursDF
      .select($"tourDifficulty", $"tourPrice", $"tourLength")
      .groupBy($"tourDifficulty")
      .agg(min($"tourPrice"), max($"tourPrice"), avg($"tourPrice"),
        min($"tourLength"), max($"tourLength"), avg($"tourLength"))
      .orderBy($"avg(tourPrice)".desc)
      .withColumn("avg(tourPrice)", regexp_replace(format_number($"avg(tourPrice)", 1), ",", ""))
      .withColumn("avg(tourLength)", format_number($"avg(tourLength)", 1))
      .show()

    // Top 10 tags for following questions
    val top10tagsDF = toursDF
      .select(explode($"tourTags").as("tourTag"))
      .groupBy($"tourTag")
      .agg(count("*").as("tagCount"))
      .orderBy($"tagCount".desc)
      .limit(10)

    // tourTag / tourDifficulty relationship for following questions
    val tagDiffRelDF = toursDF
      .select(explode($"tourTags").as("tourTag"), $"tourDifficulty", $"tourPrice")
      .groupBy($"tourTag", $"tourDifficulty")
      .agg(count("*").as("count"),
        min($"tourPrice").as("min"), max($"tourPrice").as("max"), avg($"tourPrice").as("avg"))

    println("5. Display the top 10 \"tourTags\"")
    top10tagsDF
      .withColumnRenamed("tagCount", "count")
      .show()

    println("6. Relationship between top 10 \"tourTags\" and \"tourDifficulty\"")
    top10tagsDF
      .join(tagDiffRelDF, "tourTag")
      .select($"tourTag", $"tourDifficulty", $"count", ($"count"/$"tagCount"*100).as("ratio"))
      .orderBy($"tagCount".desc, $"tourTag", $"tourDifficulty")
      .withColumn("ratio", format_number($"ratio", 1))
      .withColumnRenamed("ratio", "ratio(%)")
      .show()

    println("7. What is the min/max/avg of price in \"tourTags\" and \"tourDifficulty\" relationship?")
    top10tagsDF
      .join(tagDiffRelDF, "tourTag")
      .select($"tourTag", $"tourDifficulty", $"min", $"max", $"avg")
      .orderBy($"avg".desc)
      .withColumn("avg", regexp_replace(format_number($"avg", 1), ",", ""))
      .show()
  }
}

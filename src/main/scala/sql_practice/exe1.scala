package sql_practice

import org.apache.spark.sql.functions.sum
import spark_helpers.SessionBuilder

object exe1 {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demoDF = spark.read
      .option("multiline", false)
      .option("mode", "PERMISSIVE")
      .json("data/input/demographie_par_commune.json")

    demoDF
      .select($"Population".as("pop"))
      .agg(sum("pop").as("France_inhabitants"))
      .show()

    demoDF
      .select("Departement","Population")
      .groupBy("Departement")
      .agg(sum("Population").as("Inhabitants"))
      .orderBy($"Inhabitants".desc)
      .withColumnRenamed("Departement", "Department")
      .limit(10)
      .show()

    val depDF = spark.read
      .csv("/home/formation/big_data/Spark/data/departements.txt")
      .select($"_c0".as("Nom"), $"_c1".as("Code"))

    demoDF
      .join(depDF, demoDF("Departement") === depDF("Code"))
      .select("Nom", "Population")
      .groupBy("Nom")
      .agg(sum("Population").as("Inhabitants"))
      .orderBy($"Inhabitants".desc)
      .withColumnRenamed("Nom", "Department")
      .limit(10)
      .show()
  }
}

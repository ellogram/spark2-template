package sql_practice

import spark_helpers.SessionBuilder

object exe2 {
  def exec2(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val s7DF = spark.read
      .option("sep", "\t")
      .csv("data/input/sample_07")
      .select($"_c0".as("Code"), $"_c1".as("Description"),
        $"_c2".as("Total_emp"), $"_c3".as("Salary"))

    val s8DF = spark.read
      .option("sep", "\t")
      .csv("data/input/sample_08")
      .select($"_c0".as("Code"), $"_c1".as("Description"),
        $"_c2".as("Total_emp"), $"_c3".as("Salary"))

    println("\nOccupations with best salaries in 2007:")
    s7DF
      .select("Description", "Salary")
      .where($"Salary" > 100000)
      .orderBy($"Salary".desc)
      .limit(10)
      .show()

    println("\nSalary growth between 2007-2008:")
    s7DF
      .join(s8DF, "Code")
      .select(s7DF("Description"), ((s8DF("Salary") - s7DF("Salary"))/s7DF("Salary")*100).as("Growth"))
      .orderBy($"Growth".desc)
      .withColumnRenamed("Growth", "Growth (%)")
      .limit(10)
      .show()

    println("\nJobs loss among top earners between 2007-2008:")
    s7DF
      .join(s8DF, "Code")
      .select(s7DF("Description"), s7DF("Salary"), s7DF("Total_emp"), s8DF("Total_emp"),
        ((s8DF("Total_emp") - s7DF("Total_emp"))/s7DF("Salary")*100).as("Loss"))
      .where(s7DF("Salary") > 100000 && s7DF("Total_emp") > s8DF("Total_emp"))
      .orderBy($"Loss".asc)
      .select("Description", "Loss")
      .withColumnRenamed("Loss", "Loss (%)")
      .limit(10)
      .show()
  }
}

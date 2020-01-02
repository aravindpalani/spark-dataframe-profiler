package com.github.aravindpalani.spark.profiler.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, desc}

object values {

  def showValues(s: DataFrame): Array[DataFrame] = {
    /*
    * column name validation:
    * Non alphanumeric will be replaced by '_'
    */
    var df = s
    for (col <- s.columns) {
      df = df.withColumnRenamed(col, col.replaceAll("[^a-zA-Z0-9]", "_"))
    }
    /*
    * Total number of records (cnt):
    * Required for calculating precentage
    */
    val cnt = df.count()
    /*
    * Each resultant dataframe contains these attributes:
    *   - attribute Name (Values)
    *   - length
    *   - frequency
    *   - percentage
    */
    df.columns.map(column => {
      df.na.fill("NULL")
        .groupBy(col(s"`$column`"))
        .agg(count(s"`$column`").alias("cnt"))
        .orderBy(desc("cnt"))
        .selectExpr(s"`$column`", s" case when `$column` = 'NULL' then 0 else length(`$column`) end as length", "cnt as frequency", s"round((cnt/$cnt)*100,2) as percentage")
    })
  }
}

package com.github.aravindpalani.spark.profiler.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, desc}

object values {

  def showValues(s: DataFrame): Array[DataFrame] = {
    /*
    * Non alphanumeric column names handled using ` character
    */
    /*
    * Total number of records (cnt):
    * Required for calculating precentage
    */
    val cnt = s.count()
    /*
    * Each resultant dataframe contains these attributes:
    *   - attribute Name (Values)
    *   - length
    *   - frequency
    *   - percentage
    */
    s.columns.map(column => {
      s.na.fill("NULL")
        .groupBy(col(s"`$column`"))
        .agg(count(s"`$column`").alias("cnt"))
        .orderBy(desc("cnt"))
        .selectExpr(s"`$column`", s" case when `$column` = 'NULL' then 0 else length(`$column`) end as length", "cnt as frequency", s"round((cnt/$cnt)*100,2) as percentage")
    })
  }
}

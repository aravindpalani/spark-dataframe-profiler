package com.github.aravindpalani.spark.profiler.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, desc}

object pattern {
  def showPattern(s: DataFrame): Array[DataFrame] = {
    /*column name validation:
    1) Non alphanumeric will be replaced by '_'*/
    var df = s
    for (col <- s.columns) {
      df = df.withColumnRenamed(col, col.replaceAll("[^a-zA-Z0-9:]", "_"))
    }
    val cnt = df.count()
    df.columns.map(column => {
      df.selectExpr(s"regexp_replace(regexp_replace(regexp_replace($column,'[0-9]','9'),'[a-z]','x'),'[A-Z]','X') as $column")
        .na.fill("NULL")
        .groupBy(col(s"$column"))
        .agg(count(s"$column").alias("cnt"))
        .orderBy(desc("cnt"))
        .selectExpr(s"$column", s" case when $column = 'NULL' then 0 else length($column) end as length", "cnt as frequency", s"round((cnt/$cnt)*100,2) as percentage")
    })
  }
}

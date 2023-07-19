package com.airwallex.airskiff.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;




public class LocalSparkPartitionByExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().master("local").appName("spark compiler")
      .getOrCreate();
    Dataset<Counter> ds = spark.createDataset(Arrays.asList(new Counter("a", 1L), new Counter("a", null), new Counter("b", 2L)), Encoders.bean(Counter.class));
    ds.createOrReplaceTempView("counters");
    var result = spark.sql("select key, count(c) over (partition by key) as `count`  from counters");
    result.show();

  }
}

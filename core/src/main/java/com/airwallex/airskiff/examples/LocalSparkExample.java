package com.airwallex.airskiff.examples;

import com.airwallex.airskiff.core.SourceStream;
import com.airwallex.airskiff.core.SummedStream;
import com.airwallex.airskiff.spark.AbstractSparkCompiler;
import com.airwallex.airskiff.spark.SparkLocalTextConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class LocalSparkExample {
  public static void main(String[] args) {
    // read file from resources
    SparkSession spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .getOrCreate();
    SparkLocalTextConfig config = new SparkLocalTextConfig("core/src/main/resources/localinput.txt");

    SummedStream<String, Counter> op = new SourceStream<>(config)
      .flatMap(x -> Arrays.asList(x.split("\\s")), String.class)
      .map(x -> new Counter(x, 1L), Counter.class)
      .keyBy(x -> x.key, String.class)
      .sum((a, b) -> new Counter(b.key, a == null ? b.c : a.c + b.c));

    Dataset<Tuple2<String, Counter>> ds = new AbstractSparkCompiler(spark).compile(op);
    ds.explain();
    ds.show();
    List<Tuple2<String, Counter>> first = ds.collectAsList();
    System.out.println(first);
  }
}

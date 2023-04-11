package com.airwallex.airskiff.examples;

import com.airwallex.airskiff.core.EventTimeBasedSlidingWindow;
import com.airwallex.airskiff.core.SourceStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.spark.SparkCompiler;
import com.airwallex.airskiff.spark.SparkLocalTextConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.time.Duration;
import java.util.Arrays;

public class LocalSparkWindowExample {
  public static void main(String[] args) {
    // read file from resources
    SparkSession spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .getOrCreate();
    SparkLocalTextConfig config = new SparkLocalTextConfig("core/src/main/resources/localinput.txt");

    Stream<Integer> op = new SourceStream<>(config)
      .flatMap(x -> Arrays.asList(x.split("\\s")), String.class)
      .map(x -> x.length(), Integer.class).keyBy(x -> x, Integer.class)
      .window(new EventTimeBasedSlidingWindow(Duration.ofSeconds(10), Duration.ofSeconds(5)), it -> {
        Integer sum = 0;
        for (Integer i : it) {
          sum += i;
          i = sum;
        }
        return it;
      }, Integer::compareTo, Integer.class).values();
    Dataset ds = new SparkCompiler(spark).compileAndDecode(op);
    ds.printSchema();
    ds.explain();
    ds.show();
  }
}

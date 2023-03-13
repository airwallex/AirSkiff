package com.airwallex.airskiff.examples;

import com.airwallex.airskiff.core.EventTimeBasedSlidingWindow;
import com.airwallex.airskiff.core.SourceStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.spark.AbstractSparkCompiler;
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

    Stream<Counter> op = new SourceStream<>(config)
      .flatMap(x -> Arrays.asList(x.split("\\s")), String.class)
      .map(x -> new Counter(x, 1L), Counter.class).keyBy(x -> x.key, String.class)
      .window(new EventTimeBasedSlidingWindow(Duration.ofSeconds(10), Duration.ofSeconds(5)), it -> {
        return it;
      }, Counter::compareTo, Counter.class).values();
    Dataset ds = new AbstractSparkCompiler(spark).compile(op);
    ds.printSchema();
    ds.explain();
    ds.show();
  }
}

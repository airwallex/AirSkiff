package com.airwallex.airskiff.examples;

import com.airwallex.airskiff.core.SourceStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.spark.AbstractSparkCompiler;
import com.airwallex.airskiff.spark.SparkLocalTextConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class LocalSparkUnion {


  public static void main(String[] args) {
    // read file from resources
    SparkSession spark = SparkSession.builder().master("local").appName("spark compiler")
      .getOrCreate();

    SparkLocalTextConfig config = new SparkLocalTextConfig("core/src/main/resources/localinput.txt");


    Stream<Counter> op1 = new SourceStream<>(config)
      .flatMap(x -> Arrays.asList(x.split("\\s")), String.class)
      .map(x -> new Counter(x, 1L), Counter.class);
    Stream<Counter> op2 = new SourceStream<>(config)
      .flatMap(x -> Arrays.asList(x.split("\\s")), String.class)
      .map(x -> new Counter(x, 1L), Counter.class);


    Dataset ds = new AbstractSparkCompiler(spark).compile(op1.union(op2));
    ds.explain();
    ds.show();
    List<Tuple2<String, Counter>> first = ds.collectAsList();
    System.out.println(first);
  }
}

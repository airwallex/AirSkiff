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

public class LocalSparkSQLExample {


  public static void main(String[] args) {
    // read file from resources
    SparkSession spark = SparkSession.builder().master("local").appName("spark compiler")
      .getOrCreate();

    SparkLocalTextConfig config = new SparkLocalTextConfig("core/src/main/resources/localinput.txt");

    Stream<Counter> op = new SourceStream<>(config)
      .flatMap(x -> Arrays.asList(x.split("\\s")), String.class)
      .map(x -> new Counter(x, 1L), Counter.class)
      .sql("SELECT key, COUNT(*) OVER (PARTITION BY key ORDER BY row_time__ RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as c FROM text",
        "text", Counter.class);

    Dataset ds = new AbstractSparkCompiler(spark).compile(op);
    ds.explain();
    ds.show();
    List<Tuple2<String, Counter>> first = ds.collectAsList();
    System.out.println(first);
  }
}

package com.airwallex.airskiff.spark;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkLocalTextConfig implements SparkConfig<String> {
  public final String path;

  public SparkLocalTextConfig(String path) {
    this.path = path;
  }

  @Override
  public Class<String> clz() {
    return null;
  }

  @Override
  public Dataset<Tuple2<Long, String>> dataset(SparkSession session) {
    Dataset<String> data = session.read().textFile(path);
    return data.map((MapFunction<String, Tuple2<Long, String>>) s -> new Tuple2<>(System.currentTimeMillis(), s), Encoders.tuple(Encoders.LONG(), Encoders.STRING()));
  }

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().master("local").appName("something").getOrCreate();
    SparkLocalTextConfig config = new SparkLocalTextConfig("core/src/main/resources/localinput.txt");
    config.dataset(spark);
  }
}

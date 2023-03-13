package com.airwallex.airskiff.examples;

import com.airwallex.airskiff.common.functions.NamedMonoid;
import com.airwallex.airskiff.spark.CustomAccumulator;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;

class AddMonoid implements NamedMonoid<Long>{

  @Override
  public Long zero() {
    return 0L;
  }

  @Override
  public Long plus(Long t1, Long t2) {
    return t1 + t2;
  }
}

public class LocalSparkAccumulator {
  public static void main(String[] args) {

    SparkSession spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .getOrCreate();

    Dataset<Long> ds = spark.createDataset(Arrays.asList(1L, 2L, 3L), Encoders.LONG());
    CustomAccumulator<Long> acc = new CustomAccumulator<>(new AddMonoid(), 0L);
    spark.sparkContext().register(acc, "customAccumulator");
    Dataset<Long> result = ds.map((MapFunction<Long, Long>) x ->
      {
        acc.add(x);
        return acc.getState();
      }
      , Encoders.LONG());
    result.show();
  }
}

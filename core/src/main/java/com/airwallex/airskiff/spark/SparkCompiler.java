package com.airwallex.airskiff.spark;

import com.airwallex.airskiff.core.api.Stream;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkCompiler extends AbstractSparkCompiler {
  public SparkCompiler(SparkSession sparkSession) {
    super(sparkSession);
  }

  public <T> Dataset compileAndDecode(Stream<T> stream) {
    Dataset<Tuple2<Long, T>> ds = compile(stream);
    Class<T> clz = stream.getClazz();
    return ds.map((MapFunction<Tuple2<Long, T>, Tuple2<Long, T>>) t -> t, Encoders.tuple(Encoders.LONG(), Utils.encodeBean(clz)));
  }

  public static void main(String[] args) {

  }
}

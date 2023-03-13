package com.airwallex.airskiff.spark;

import com.airwallex.airskiff.core.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public interface SparkConfig<T> extends Config<T> {


  Dataset<Tuple2<Long, T>> source(SparkSession session);

}

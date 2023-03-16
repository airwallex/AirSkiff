package com.airwallex.airskiff.testhelpers;

import com.airwallex.airskiff.spark.SparkConfig;
import com.airwallex.airskiff.spark.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

public class TestSparkConfig<T> implements SparkConfig<T> {

  private final List<Tuple2<Long, T>> _data;
  private final Class<T> _tc;

  public TestSparkConfig(List<Tuple2<Long, T>> _data, Class<T> _tc) {
    this._data = _data;
    this._tc = _tc;
  }

  @Override
  public Class<T> clz() {
    return _tc;
  }

  @Override
  public Dataset<Tuple2<Long, T>> dataset(SparkSession session) {
    System.out.println(_data);
    Dataset<Tuple2<Long, T>> ds = session.createDataset(_data, Encoders.tuple(Encoders.LONG(), Utils.encode(_tc)));
    return ds;
  }
}

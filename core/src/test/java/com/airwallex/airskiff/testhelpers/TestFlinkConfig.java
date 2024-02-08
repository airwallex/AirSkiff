package com.airwallex.airskiff.testhelpers;

import com.airwallex.airskiff.flink.FlinkConfig;
import com.airwallex.airskiff.flink.Utils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;

public class TestFlinkConfig<T> implements FlinkConfig<T> {
  private final List<Tuple2<Long, T>> _data;
  private final Class<T> _tc;

  public TestFlinkConfig(List<Tuple2<Long, T>> data, Class<T> tc) {
    _data = data;
    _tc = tc;
  }

  @Override
  public Class<T> clz() {
    return _tc;
  }

  @Override
  public DataStream<Tuple2<Long, T>> source(StreamExecutionEnvironment env, boolean isBatch) {
    return env.fromCollection(_data).assignTimestampsAndWatermarks(Utils.watermark(isBatch, Duration.ZERO));
  }
}

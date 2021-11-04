package com.airwallex.airskiff.flink;

import com.airwallex.airskiff.core.config.Config;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface FlinkConfig<T> extends Config<T> {
  DataStream<Tuple2<Long, T>> source(StreamExecutionEnvironment env, boolean isBatch);
}

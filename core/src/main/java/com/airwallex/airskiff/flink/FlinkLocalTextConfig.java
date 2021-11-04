package com.airwallex.airskiff.flink;

import static com.airwallex.airskiff.flink.Utils.tuple2TypeInfo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkLocalTextConfig implements FlinkConfig<String> {
  private final String path;

  public FlinkLocalTextConfig(String path) {
    this.path = path;
  }

  @Override
  public Class<String> clz() {
    return String.class;
  }

  @Override
  public DataStream<Tuple2<Long, String>> source(
    StreamExecutionEnvironment env, boolean isBatch
  ) {
    DataStream<String> stream;
    if (isBatch) {
      stream = env.readTextFile(path);
    } else {
      stream = env.socketTextStream("localhost", 10000);
    }
    return stream.map(t -> new Tuple2<>(System.currentTimeMillis(), t), tuple2TypeInfo(String.class))
      .assignTimestampsAndWatermarks(Utils.watermark(isBatch));
  }
}

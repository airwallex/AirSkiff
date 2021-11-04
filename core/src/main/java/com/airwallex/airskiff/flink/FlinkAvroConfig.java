package com.airwallex.airskiff.flink;

import com.airwallex.airskiff.common.GcsDailyLocation;
import com.airwallex.airskiff.common.functions.SerializableLambda;
import com.airwallex.airskiff.flink.types.AvroSpecificTypeInfo;
import java.time.Instant;
import java.util.Map;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkAvroConfig<T extends SpecificRecordBase> implements FlinkConfig<T> {
  private Path path;
  private OnlineConfig<T> onlineConfig;
  private final GcsDailyLocation location;
  private final SerializableLambda<T, Instant> timeExtractor;
  private final Class<T> tc;

  public FlinkAvroConfig(
    GcsDailyLocation location, SerializableLambda<T, Instant> timeExtractor, Class<T> tc
  ) {
    this.location = location;
    this.timeExtractor = timeExtractor;
    this.tc = tc;
  }

  public FlinkAvroConfig(
    GcsDailyLocation location, SerializableLambda<T, Instant> timeExtractor, Class<T> tc, OnlineConfig<T> onlineConfig
  ) {
    this.onlineConfig = onlineConfig;
    this.location = location;
    this.timeExtractor = timeExtractor;
    this.tc = tc;
  }

  /**
   * This should only be used in adhoc jobs to override the production path
   */
  public void setPath(Path p) {
    assert p != null;
    path = p;
  }

  public void updateProperties(Map<String, String> props) {
    if (onlineConfig != null) {
      onlineConfig.updateProperties(props);
    }
  }

  @Override
  public DataStream<Tuple2<Long, T>> source(StreamExecutionEnvironment env, boolean isBatch) {
    DataStreamSource<T> stream;
    if (isBatch) {
      if (path == null && location == null) {
        throw new IllegalArgumentException("Batch location is not set!");
      }
      if (path == null) {
        path = new Path(location.latestPath());
      }
      AvroInputFormat<T> inputFormat = new AvroInputFormat<>(path, tc);
      stream = env.createInput(inputFormat, new AvroSpecificTypeInfo<>(tc));
    } else {
      if (onlineConfig == null) {
        throw new IllegalArgumentException("Online config is not set!");
      }
      if (onlineConfig instanceof KafkaAvroConfig) {
        KafkaAvroConfig<T> config = (KafkaAvroConfig<T>) onlineConfig;
        stream = env.addSource(config.toConsumer());
      } else {
        throw new IllegalArgumentException("Unknown online config type: " + onlineConfig.getClass().getName());
      }
    }
    return stream
      .map(t -> new Tuple2<>(timeExtractor.apply(t).toEpochMilli(), t), Utils.tuple2TypeInfo(tc))
      .assignTimestampsAndWatermarks(Utils.watermark(isBatch));
  }

  @Override
  public Class<T> clz() {
    return tc;
  }
}

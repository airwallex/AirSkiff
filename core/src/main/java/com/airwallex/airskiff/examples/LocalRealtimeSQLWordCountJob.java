package com.airwallex.airskiff.examples;

import com.airwallex.airskiff.core.SourceStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.flink.FlinkLocalTextConfig;
import com.airwallex.airskiff.flink.FlinkRealtimeCompiler;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;


interface MetricData extends com.airwallex.airskiff.common.functions.SerializableComparable<Object>, com.airwallex.airskiff.common.functions.Id {
  String valueType();

  String value();
}

class Counter2 implements MetricData {
  public String key;
  public Long count;

  @Override
  public String id() {
    return key;
  }

  public Counter2(String id, Long amount) {
    this.key = id;
    this.count = amount;
  }

  @Override
  public String valueType() {
    return Integer.class.getName();
  }

  @Override
  public String value() {
    return count.toString();
  }

  @Override
  public int compareTo(Object o) {
    return 0;
  }
}


public class LocalRealtimeSQLWordCountJob {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    env.setParallelism(2);
    Path inputPath = Paths.get("core/src/main/resources/localInput.txt");
    // default host and port is localhost:10000 for socket stream
    FlinkLocalTextConfig config = new FlinkLocalTextConfig(inputPath.toAbsolutePath().toString());
    SourceStream<String> source = new SourceStream<>(config);

    Stream<Counter> stream = source.flatMap(x -> Arrays.asList(x.split("\\s")), String.class)
      .map(x ->
      {
        System.out.println(x);
        return new Counter(x, 1L);
      }, Counter.class)
      .sql(
        "SELECT key, COUNT(*) OVER (PARTITION BY key ORDER BY row_time__ RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM text",
        "text",
        Counter.class
      );

    new FlinkRealtimeCompiler(env, tableEnv).compile(stream)
      .process(new ProcessFunction<Tuple2<Long, Counter>, Object>() {
        @Override
        public void processElement(Tuple2<Long, Counter> t, ProcessFunction<Tuple2<Long, Counter>, Object>.Context ctx, Collector<Object> out) throws Exception {
          System.out.println(t);
          long now = System.currentTimeMillis();
          long e2eDurationInMs = now - t.f0;
          System.out.println("e2eDurationInMs: " + e2eDurationInMs);
        }
      });
    env.execute();
  }
}

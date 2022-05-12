package com.airwallex.airskiff.examples;

import com.airwallex.airskiff.core.SourceStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.flink.FlinkBatchCompiler;
import com.airwallex.airskiff.flink.FlinkLocalTextConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class LocalBatchWordCountJob {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    Path inputPath = Paths.get("core/src/main/resources/localInput.txt");
    FlinkLocalTextConfig config = new FlinkLocalTextConfig(inputPath.toAbsolutePath().toString());
    Stream<Counter> stream = new SourceStream<>(config)
      .flatMap(x -> Arrays.asList(x.split("\\s")), String.class)
      .map(x -> new Counter(x, 1L), Counter.class)
      .keyBy(x -> x.key, String.class)
      .sum((a, b) -> new Counter(b.key, a == null ? 0 : a.c + b.c))
      .values();

    new FlinkBatchCompiler(env, tableEnv).compile(stream).print();
    env.execute();
  }
}

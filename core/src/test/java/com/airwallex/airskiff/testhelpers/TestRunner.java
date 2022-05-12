package com.airwallex.airskiff.testhelpers;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.core.SourceStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.flink.FlinkBatchCompiler;
import com.airwallex.airskiff.flink.FlinkRealtimeCompiler;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Assertions;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestRunner {
  public final EnvironmentSettings fsSettings;
  public final StreamExecutionEnvironment env;
  public final StreamTableEnvironment tableEnv;
  public final FlinkBatchCompiler batchCompiler;
  public final FlinkRealtimeCompiler realtimeCompiler;
  public final TestCompiler testCompiler;

  public TestRunner() {
    this.fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    this.env = StreamExecutionEnvironment.getExecutionEnvironment();
    this.tableEnv = StreamTableEnvironment.create(env, fsSettings);

    this.batchCompiler = new FlinkBatchCompiler(env, tableEnv);
    this.realtimeCompiler = new FlinkRealtimeCompiler(env, tableEnv);
    this.testCompiler = new TestCompiler();

    // To make sure all inputs and outputs are aligned.
    env.setParallelism(1);
  }

  public <T> List<Pair<Long, T>> toPairs(List<Tuple2<Long, T>> data) {
    return data.stream().map(t -> new Pair<>(t.f0, t.f1)).collect(Collectors.toList());
  }

  public <T> List<Tuple2<Long, T>> runFlinkBatch(Stream<T> s, int limit) throws Exception {
    return batchCompiler.compile(s).executeAndCollect(Math.max(limit, 10000));
  }

  public <T> List<Tuple2<Long, T>> runFlinkRealtime(Stream<T> s, int limit) throws Exception {
    return realtimeCompiler.compile(s).executeAndCollect(Math.max(limit, 10000));
  }

  public <T> List<Pair<Long, T>> runLocal(Stream<T> s) {
    return testCompiler.compile(s);
  }

  public <T> void executeAndCheck(
    Function<Stream<TestInputData>, Stream<T>> f, List<Tuple2<Long, TestInputData>> data
  ) throws Exception {
    executeAndCheck(f, data, false, true);
  }

  public <T> void executeAndCheck(
    Function<Stream<TestInputData>, Stream<T>> f,
    List<Tuple2<Long, TestInputData>> data,
    boolean sort,
    boolean ignoreRealtime
  ) throws Exception {
    data.sort(Comparator.comparing(t -> t.f0));
    var config = new TestFlinkConfig<>(data, TestInputData.class);
    var source = new SourceStream<>(config);
    var flinkBatchList = runFlinkBatch(f.apply(source), data.size());
    var flinkRealtimeList = runFlinkRealtime(f.apply(source), data.size());

    var listConfig = new ListSourceConfig<>(toPairs(data), TestInputData.class);
    var listSource = new SourceStream<>(listConfig);
    var l2 = runLocal(f.apply(listSource));

    Assertions.assertEquals(flinkBatchList.size(), l2.size());
    Assertions.assertEquals(flinkRealtimeList.size(), l2.size());

    if (sort) {
      // only sort the key `b`, not `a`
      flinkBatchList.sort(Comparator.comparing(o -> {
        var i = (TestInputData) o.f1;
        return i.b;
      }));
      flinkRealtimeList.sort(Comparator.comparing(o -> {
        var i = (TestInputData) o.f1;
        return i.b;
      }));
      l2.sort(Comparator.comparing(o -> {
        var i = (TestInputData) o.r;
        return i.b;
      }));
    }

    for (int i = 0; i < flinkBatchList.size(); i++) {
      Tuple2<Long, T> t1 = flinkBatchList.get(i);
      Tuple2<Long, T> t2 = flinkRealtimeList.get(i);
      Pair<Long, T> p = l2.get(i);
      if (!ignoreRealtime) {
        Assertions.assertEquals(t1, t2);
      }
      Assertions.assertEquals(t1.f0, p.l);
      Assertions.assertEquals(t1.f1, p.r);
    }
  }
}

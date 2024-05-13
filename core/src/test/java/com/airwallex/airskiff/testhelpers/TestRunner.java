package com.airwallex.airskiff.testhelpers;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.core.SourceStream;
import com.airwallex.airskiff.core.StreamUtils;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.flink.FlinkBatchCompiler;
import com.airwallex.airskiff.flink.FlinkRealtimeCompiler;
import com.airwallex.airskiff.spark.AbstractSparkCompiler;
import com.airwallex.airskiff.spark.SparkCompiler;
import com.airwallex.airskiff.spark.Utils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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
  private static final Logger logger = LoggerFactory.getLogger(TestRunner.class);

  public TestRunner() {

    Configuration configuration = new Configuration();
// set low-level key-value options
    configuration.setString("table.exec.mini-batch.enabled", "true");
    configuration.setString("table.exec.mini-batch.allow-latency", "20 ms");
//    configuration.setString("table.exec.mini-batch.size", "5000");
    this.fsSettings = EnvironmentSettings.newInstance().inStreamingMode().withConfiguration(configuration).build();
    this.env = StreamExecutionEnvironment.getExecutionEnvironment();
    this.env.setBufferTimeout(5);
    this.tableEnv = StreamTableEnvironment.create(env, fsSettings);
    this.batchCompiler = new FlinkBatchCompiler(env, tableEnv);
    this.realtimeCompiler = new FlinkRealtimeCompiler(env, tableEnv, Duration.ZERO, Duration.ofMillis(300));
    this.testCompiler = new TestCompiler();

    // To make sure all inputs and outputs are aligned.
    env.setParallelism(1);
  }

  public <T> List<Pair<Long, T>> toPairs(List<Tuple2<Long, T>> data) {
    return data.stream().map(t -> new Pair<>(t.f0, t.f1)).collect(Collectors.toList());
  }

  public <T> List<Tuple2<Long, T>> runFlinkBatch(Stream<T> s, int limit) throws Exception {
    var start = System.currentTimeMillis();
    var stream = batchCompiler.compile(s);
    logger.debug("batch compile time: " + (System.currentTimeMillis() - start));
    start = System.currentTimeMillis();
    var result = stream.executeAndCollect(Math.max(limit, 10000));
    logger.debug("batch execute time: " + (System.currentTimeMillis() - start));
    return result;
  }

  public <T> List<Tuple2<Long, T>> runFlinkRealtime(Stream<T> s, int limit) throws Exception {
    var start = System.currentTimeMillis();
    var stream = realtimeCompiler.compile(s);
    logger.debug("realtime compile time: " + (System.currentTimeMillis() - start));
    var result = stream.executeAndCollect(Math.max(limit, 10000));
    logger.debug("realtime execute time: " + (System.currentTimeMillis() - start));
    return result;
  }

  public <T> List<scala.Tuple2<Long, T>> runSpark(Stream<T> s, int limit) throws Exception {
    SparkSession sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("tests")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate();
    Class<T> clz = StreamUtils.clz(s);
    Dataset<scala.Tuple2<Long, T>> ds = new SparkCompiler(sparkSession).compile(s);
    // This is purely for fixing different serialization issues.
    // For SQL to work, we have to use Encoders.bean
    // For Avro to work we have to use Java serialization, TODO try KRYO
    Dataset<scala.Tuple2<Long, T>> result = ds.map((MapFunction<scala.Tuple2<Long, T>, scala.Tuple2<Long, T>>) t -> t,
      Encoders.tuple(Encoders.LONG(), Utils.encode(clz)));
    result.show();
//    Dataset<scala.Tuple2<Long, T>> newResult = result.map((MapFunction<scala.Tuple2<Long, T>, scala.Tuple2<Long, T>>) t -> {
//      System.out.println(t._1);
//      System.out.println(t._2);
//      return new scala.Tuple2<>(t._1 + 1L, t._2);
//    }, Encoders.tuple(Encoders.LONG(), Utils.encode(clz)));
    List<scala.Tuple2<Long, T>> data = result.collectAsList();
    return data.subList(0, Math.min(limit, data.size()));
  }

  public <T> List<Pair<Long, T>> runLocal(Stream<T> s) {
    return testCompiler.compile(s);
  }

  public <T> void executeAndCheck(
    Function<Stream<TestInputData>, Stream<T>> f, List<Tuple2<Long, TestInputData>> data
  ) throws Exception {
    executeAndCheck(f, data, false, true);
  }

  public List<scala.Tuple2<Long, TestInputData>> toSparkData(List<Tuple2<Long, TestInputData>> data) {
    return data.stream().map(
      t -> new scala.Tuple2<>(t.f0, new TestInputData(t.f1.a, t.f1.b))).collect(Collectors.toList());
  }

  public <T> void executeAndCheck(
    Function<Stream<TestInputData>, Stream<T>> f,
    List<Tuple2<Long, TestInputData>> data,
    boolean sort,
    boolean ignoreRealtime
  ) throws Exception {
    data.sort(Comparator.comparing(t -> t.f0));
    List<scala.Tuple2<Long, TestInputData>> sparkData = toSparkData(data);
    var config = new TestFlinkConfig<>(data, TestInputData.class);
    var sparkConfig = new TestSparkConfig<>(sparkData, TestInputData.class);
    var source = new SourceStream<>(config);
    var sparkSource = new SourceStream<>(sparkConfig);
    var flinkBatchList = runFlinkBatch(f.apply(source), data.size());
    var flinkRealtimeList = runFlinkRealtime(f.apply(source), data.size());
    var sparkList = runSpark(f.apply(sparkSource), sparkData.size());

    var listConfig = new ListSourceConfig<>(toPairs(data), TestInputData.class);
    var listSource = new SourceStream<>(listConfig);
    var l2 = runLocal(f.apply(listSource));

    Assertions.assertEquals(flinkBatchList.size(), l2.size());
    Assertions.assertEquals(flinkRealtimeList.size(), l2.size());
//    Assertions.assertEquals(sparkList.size(), l2.size());

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

      sparkList.sort(Comparator.comparing(o -> {
        var i = (TestInputData) o._2;
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
//      scala.Tuple2<Long, T> t3 = sparkList.get(i);

      Pair<Long, T> p = l2.get(i);
      if (!ignoreRealtime) {
        Assertions.assertEquals(t1, t2);
//        Assertions.assertEquals(t2.f0, t3._1);
//        Assertions.assertEquals(t2.f1, t3._2);
      }
      Assertions.assertEquals(t1.f0, p.l);
      Assertions.assertEquals(t1.f1, p.r);
    }

    if (flinkBatchList.size() > 0) {
      Tuple2<Long, T> t2 = flinkBatchList.get(flinkBatchList.size() - 1);
      scala.Tuple2<Long, T> t3 = sparkList.get(sparkList.size() - 1);
      Assertions.assertEquals(t2.f0, t3._1);
      Assertions.assertEquals(t2.f1, t3._2);
    }
  }
}

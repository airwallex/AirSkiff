package com.airwallex.airskiff;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class StatePerformanceTest {

  private StreamExecutionEnvironment env;
  private static final int DATA_SIZE = 1000000;
  private static final int NUM_KEYS = 1000;
  private static final int WARMUP_ITERATIONS = 1;
  private static final int BENCHMARK_ITERATIONS = 5;

  @BeforeEach
  public void setUp() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // Set parallelism to 1 for consistent results
  }

  @Test
  public void testMapStateVsListState() throws Exception {
    List<Tuple2<Integer, Integer>> inputData = generateData();

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      runTest(inputData, true);
      runTest(inputData, false);
    }

    // Actual benchmarking
    List<Long> mapStateTimes = new ArrayList<>();
    List<Long> listStateTimes = new ArrayList<>();

    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      mapStateTimes.add(runTest(inputData, true));
      listStateTimes.add(runTest(inputData, false));

      // Force garbage collection between runs
      System.gc();
      TimeUnit.SECONDS.sleep(1);
    }

    // Calculate and print results
    printResults("MapState", mapStateTimes);
    printResults("ListState", listStateTimes);
  }

  private void printResults(String stateName, List<Long> times) {
    double average = times.stream().mapToLong(Long::longValue).average().orElse(0.0);
    long min = times.stream().mapToLong(Long::longValue).min().orElse(0);
    long max = times.stream().mapToLong(Long::longValue).max().orElse(0);

    System.out.println(stateName + " execution times:");
    System.out.println("  Average: " + average + " ms");
    System.out.println("  Min: " + min + " ms");
    System.out.println("  Max: " + max + " ms");
    System.out.println("  All times: " + times);
  }

  private long runTest(List<Tuple2<Integer, Integer>> inputData, boolean useMapState) throws Exception {
    DataStream<Tuple2<Integer, Integer>> stream = env.fromCollection(inputData);

    CollectSink sink = new CollectSink();

    long startTime = System.nanoTime();

    if (useMapState) {
      stream.keyBy(t -> t.f0)
        .map(new MapStateFunction())
        .addSink(sink);
    } else {
      stream.keyBy(t -> t.f0)
        .map(new ListStateFunction())
        .addSink(sink);
    }

    env.execute();

    long endTime = System.nanoTime();
    return TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
  }

  private List<Tuple2<Integer, Integer>> generateData() {
    List<Tuple2<Integer, Integer>> data = new ArrayList<>(DATA_SIZE);
    Random random = new Random();
    for (int i = 0; i < DATA_SIZE; i++) {
      data.add(new Tuple2<>(random.nextInt(NUM_KEYS), random.nextInt(1000)));
    }
    return data;
  }

  private static class MapStateFunction extends RichMapFunction<Tuple2<Integer, Integer>, Integer> {
    private transient MapState<Integer, Integer> state;

    @Override
    public void open(Configuration parameters) throws Exception {
      MapStateDescriptor<Integer, Integer> descriptor =
        new MapStateDescriptor<>("map-state", Integer.class, Integer.class);
      state = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public Integer map(Tuple2<Integer, Integer> value) throws Exception {
      Integer stored = state.get(value.f0);
      if (stored == null) {
        stored = 0;
      }
      int updated = stored + value.f1;
      state.put(value.f0, updated);
      return updated;
    }
  }

  private static class ListStateFunction extends RichMapFunction<Tuple2<Integer, Integer>, Integer> {
    private transient ListState<Tuple2<Integer, Integer>> state;

    @Override
    public void open(Configuration parameters) throws Exception {
      ListStateDescriptor<Tuple2<Integer, Integer>> descriptor =
        new ListStateDescriptor<>("list-state", TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {}));
      state = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public Integer map(Tuple2<Integer, Integer> value) throws Exception {
      Iterable<Tuple2<Integer, Integer>> current = state.get();
      int sum = value.f1;
      for (Tuple2<Integer, Integer> t : current) {
        if (t.f0.equals(value.f0)) {
          sum += t.f1;
        }
      }
      state.add(new Tuple2<>(value.f0, sum));
      return sum;
    }
  }

  private static class CollectSink implements SinkFunction<Integer> {
    public static final List<Integer> values = new ArrayList<>();

    @Override
    public synchronized void invoke(Integer value, Context context) {
      values.add(value);
    }
  }
}

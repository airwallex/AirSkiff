package com.airwallex.airskiff.flink;

import com.airwallex.airskiff.testhelpers.TestRunner;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlinkTest implements Serializable {
  static int count = 0;
  static List<Integer> outputRes = new ArrayList<>();

  /**
   * This is a test that would fail with Kotlin
   */
  @Test
  public void testFlinkSqlApi() {
    TestRunner runner = new TestRunner();
    DataStream<Integer> d = runner.env.fromCollection(Arrays.asList(1, 2, 3));
    runner.tableEnv.createTemporaryView("abc", d);
    runner.tableEnv.sqlQuery("select * from abc");
  }

  @Test
  public void testRealtimeEventTimeTrigger() throws Exception {
    List<Tuple2<Long, Integer>> l = new ArrayList<>();
    l.add(new Tuple2<>(1L, 0));
    l.add(new Tuple2<>(2L, 1));
    l.add(new Tuple2<>(Duration.ofDays(7).toMillis(), 2));

    TestRunner runner = new TestRunner();
    runner.env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    DataStreamSource<Tuple2<Long, Integer>> d = runner.env.fromCollection(l);
    d.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<Long, Integer>>forMonotonousTimestamps().withTimestampAssigner(
        (t, x) -> t.f0))
      .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      .trigger(RealtimeEventTimeTrigger.create())
      .process(new ProcessAllWindowFunction<Tuple2<Long, Integer>, Integer, TimeWindow>() {
        @Override
        public void process(
          Context context, Iterable<Tuple2<Long, Integer>> iterable, Collector<Integer> collector
        ) throws Exception {
          count++;
          for (Tuple2<Long, Integer> t : iterable) {
            collector.collect(t.f1);
          }
        }
      })
      .addSink(new SinkFunction<Integer>() {
        @Override
        public void invoke(Integer value) throws Exception {
          outputRes.add(value);
        }
      });
    runner.env.execute();

    Assertions.assertEquals(count, 3);
    Assertions.assertEquals(outputRes.size(), 4);
    Assertions.assertEquals(outputRes.get(0), 0);
    Assertions.assertEquals(outputRes.get(1), 0);
    Assertions.assertEquals(outputRes.get(2), 1);
    Assertions.assertEquals(outputRes.get(3), 2);
  }

  @Test
  public void testSideOutput() throws Exception {
    List<Tuple2<Integer, Integer>> l = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      l.add(new Tuple2<>(i, i));
    }
    TestRunner runner = new TestRunner();
    runner.env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    DataStreamSource<Tuple2<Integer, Integer>> input = runner.env.fromCollection(l);

    final OutputTag<Integer> outputTag = new OutputTag<>("side-output") {
    };

    SingleOutputStreamOperator<Integer> mainDataStream =
      input.process(new ProcessFunction<Tuple2<Integer, Integer>, Integer>() {

        @Override
        public void processElement(
          Tuple2<Integer, Integer> value, Context ctx, Collector<Integer> out
        ) throws Exception {
          if (value.f0 > 4) {
            // emit data to regular output
            out.collect(value.f1);
          } else {
            // emit data to side output
            ctx.output(outputTag, value.f1);
          }
        }
      });

    DataStream<Integer> sideOutputStream = mainDataStream.getSideOutput(outputTag);
    List<Integer> mainData = mainDataStream.executeAndCollect(10);
    List<Integer> sideData = sideOutputStream.executeAndCollect(10);

    Assertions.assertEquals(mainData.size(), 5);
    for (int i = 0; i < mainData.size(); i++) {
      Assertions.assertEquals(mainData.get(i), i + 5);
    }
    Assertions.assertEquals(sideData.size(), 5);
    for (int i = 0; i < sideData.size(); i++) {
      Assertions.assertEquals(sideData.get(i), i);
    }
  }
}

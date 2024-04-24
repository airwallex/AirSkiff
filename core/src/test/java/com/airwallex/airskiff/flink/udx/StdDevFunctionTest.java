package com.airwallex.airskiff.flink.udx;

import com.google.common.math.DoubleMath;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class StdDevFunctionTest {
  public static MiniClusterWithClientResource flinkCluster;
  public StreamExecutionEnvironment env;
  public StreamTableEnvironment tableEnv;
  private static final double tolerance = 1.0E-9;

  @Before
  public void setup() throws Exception {
    flinkCluster = new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberSlotsPerTaskManager(1)
        .setNumberTaskManagers(1)
        .build());
    flinkCluster.before();
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    tableEnv = StreamTableEnvironment.create(env);
    tableEnv.createTemporarySystemFunction("ASStddev", StdDevFunction.class);
  }

  @Test
  public void testSQL() throws Exception {
    Sink.values.clear();
    DataStream<Tuple3<Long, String, Double>> source = env.fromElements(
      new Tuple3(1708403000000L, "a", 3.0),    // null
      new Tuple3(1708403300000L, "a", 6.0),    // 2.1213...
      new Tuple3(1708403600000L, "a", 9.0),    // stddev([3.0, 6.0, 9.0]) = 3.0
      new Tuple3(1708403900000L, "a", 12.0)    // stddev([6.0, 9.0, 12.0]) = 3.0
    );
    DataStream<Tuple3<Long, String, Double>> ds = source.assignTimestampsAndWatermarks(
      WatermarkStrategy.<Tuple3<Long, String, Double>>forMonotonousTimestamps().withTimestampAssigner(
        (t, x) -> t.f0)
    );
    tableEnv.createTemporaryView("tmp", ds, $("f0"), $("f1"), $("f2"), $("f0").rowtime().as("row_time"));

    String sql =
      "SELECT f0, f1 " +
        " ,ASStddev(f2) OVER (PARTITION BY f1 ORDER BY row_time RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW) " +
        " FROM tmp";
    Table t = tableEnv.sqlQuery(sql);
    tableEnv.toDataStream(t).addSink(new Sink());
    env.execute();
    assertEquals(4, Sink.values.size());
    assertEquals(1708403000000L, Sink.values.get(0).getField(0));
    assertEquals("a", Sink.values.get(0).getField(1));
    assertEquals(null, Sink.values.get(0).getField(2));
    assertEquals(1708403600000L, Sink.values.get(2).getField(0));
    assertEquals("a", Sink.values.get(2).getField(1));
    assertEquals(3.0, Sink.values.get(2).getField(2));
    assertEquals(1708403900000L, Sink.values.get(3).getField(0));
    assertEquals("a", Sink.values.get(3).getField(1));
    assertEquals(3.0, Sink.values.get(3).getField(2));
  }

  // ASStddev have little difference with stddev in FlinkSQL standard library
  @Test
  public void compareWithLibStddev() throws Exception {
    Sink.values.clear();
    List<Tuple3<Long, String, Double>> list = new ArrayList<>();
    for(int i = 0; i < 1000; i++) {
      list.add(new Tuple3(1708403000000L + i*60000, "a", ((int)(Math.random() * 100))*1.0));
    }
    DataStream<Tuple3<Long, String, Double>> source = env.fromCollection(list);
    DataStream<Tuple3<Long, String, Double>> ds = source.assignTimestampsAndWatermarks(
      WatermarkStrategy.<Tuple3<Long, String, Double>>forMonotonousTimestamps().withTimestampAssigner(
        (t, x) -> t.f0)
    );
    tableEnv.createTemporaryView("tmp", ds, $("f0"), $("f1"), $("f2"), $("f0").rowtime().as("row_time"));

    String sql =
      "SELECT f0, f1 " +
        " ,ASStddev(f2) OVER (PARTITION BY f1 ORDER BY row_time RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW) " +
        " ,stddev(f2) OVER (PARTITION BY f1 ORDER BY row_time RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW) " +
        " FROM tmp";
    Table t = tableEnv.sqlQuery(sql);
    tableEnv.toDataStream(t).addSink(new Sink());
    env.execute();
    for(int i = 1; i < 1000; i++) {
      assertEquals(true, DoubleMath.fuzzyEquals(
        (double)Sink.values.get(i).getField(2),
        (double)Sink.values.get(i).getField(3),
        tolerance)
      );
    }
  }

  // ASStddev returns a small number when all numbers are same, while stddev returns NaN
  @Test
  public void diffWithLibStddev() throws Exception {
    Sink.values.clear();
    List<Tuple3<Long, String, Double>> list = new ArrayList<>();
    for(int i = 0; i < 1000; i++) {
      list.add(new Tuple3(1708403000000L + i*1, "a", 0.001));
    }
    DataStream<Tuple3<Long, String, Double>> source = env.fromCollection(list);
    DataStream<Tuple3<Long, String, Double>> ds = source.assignTimestampsAndWatermarks(
      WatermarkStrategy.<Tuple3<Long, String, Double>>forMonotonousTimestamps().withTimestampAssigner(
        (t, x) -> t.f0)
    );
    tableEnv.createTemporaryView("tmp", ds, $("f0"), $("f1"), $("f2"), $("f0").rowtime().as("row_time"));

    String sql =
      "SELECT f0, f1 " +
        " ,ASStddev(f2) OVER (PARTITION BY f1 ORDER BY row_time RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW) " +
        " ,stddev(f2) OVER (PARTITION BY f1 ORDER BY row_time RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW) " +
        " FROM tmp";
    Table t = tableEnv.sqlQuery(sql);
    tableEnv.toDataStream(t).addSink(new Sink());
    env.execute();
    int ASStddevNaNCnt = 0;
    int stddevNaNCnt = 0;
    for(int i = 1; i < 1000; i++) {
      if(Double.isNaN((Double)Sink.values.get(i).getField(2))) {
        ASStddevNaNCnt++;
      }
      assertEquals(true, DoubleMath.fuzzyEquals(
        (double)Sink.values.get(i).getField(2),
        0.0,
        tolerance)
      );
      if(Double.isNaN((Double)Sink.values.get(i).getField(3))) {
        stddevNaNCnt++;
      }
    }
    assertEquals(0, ASStddevNaNCnt);
    assertNotEquals(0, stddevNaNCnt);
  }

  @Test
  public void testSQLWithNull() throws Exception {
    Sink.values.clear();
    DataStream<Tuple3<Long, String, Double>> source = env.fromElements(
      new Tuple3(1708403000001L, "a", 3.0),    // null
      new Tuple3(1708403000002L, "a", -1.0),   // null
      new Tuple3(1708403300000L, "a", 6.0),    // stddev([3.0, 6.0]) = 2.1213...
      new Tuple3(1708403300001L, "a", -1.0),   // stddev([3.0, 6.0]) = 2.1213...
      new Tuple3(1708403600000L, "a", 9.0),    // stddev([3.0, 6.0, 9.0]) = 3.0
      new Tuple3(1708403600001L, "a", -1.0)    // stddev([3.0, 6.0, 9.0]) = 3.0
    );
    DataStream<Tuple3<Long, String, Double>> ds = source.assignTimestampsAndWatermarks(
      WatermarkStrategy.<Tuple3<Long, String, Double>>forMonotonousTimestamps().withTimestampAssigner(
        (t, x) -> t.f0)
    );
    tableEnv.createTemporaryView("tmp", ds, $("f0"), $("f1"), $("f2"), $("f0").rowtime().as("row_time"));

    String sql =
      "SELECT f0, f1 " +
        " ,ASStddev(case when f2 = -1.0 then null else f2 end) OVER (PARTITION BY f1 ORDER BY row_time RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW) " +
        "   ,stddev(case when f2 = -1.0 then null else f2 end) OVER (PARTITION BY f1 ORDER BY row_time RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW) " +
        " FROM tmp";
    Table t = tableEnv.sqlQuery(sql);
    tableEnv.toDataStream(t).addSink(new Sink());
    env.execute();
//    [1708403000001, a, null, null]
//    [1708403000002, a, null, null]
//    [1708403300000, a, 2.12132034..., 2.12132034...]
//    [1708403300001, a, 2.12132034..., 2.12132034...]
//    [1708403600000, a, 3.0, 3.0]
//    [1708403600001, a, 3.0, 3.0]
    assertEquals(6, Sink.values.size());
    assertEquals(1708403000001L, Sink.values.get(0).getField(0));
    assertEquals("a", Sink.values.get(0).getField(1));
    assertEquals(null, Sink.values.get(0).getField(2));
    assertEquals(Sink.values.get(0).getField(2), Sink.values.get(0).getField(3));

    assertEquals(1708403000002L, Sink.values.get(1).getField(0));
    assertEquals("a", Sink.values.get(1).getField(1));
    assertEquals(null, Sink.values.get(1).getField(2));
    assertEquals(Sink.values.get(1).getField(2), Sink.values.get(1).getField(3));

    assertEquals(Sink.values.get(2).getField(1), Sink.values.get(3).getField(1));
    assertEquals(Sink.values.get(2).getField(2), Sink.values.get(2).getField(3));
    assertEquals(Sink.values.get(3).getField(2), Sink.values.get(3).getField(3));
    assertEquals(Sink.values.get(2).getField(2), Sink.values.get(3).getField(2));

    assertEquals(1708403600000L, Sink.values.get(4).getField(0));
    assertEquals("a", Sink.values.get(4).getField(1));
    assertEquals(3.0, Sink.values.get(4).getField(2));
    assertEquals(Sink.values.get(4).getField(2), Sink.values.get(4).getField(3));

    assertEquals(1708403600001L, Sink.values.get(5).getField(0));
    assertEquals("a", Sink.values.get(5).getField(1));
    assertEquals(3.0, Sink.values.get(5).getField(2));
    assertEquals(Sink.values.get(5).getField(2), Sink.values.get(5).getField(3));
  }

  @After
  public void tearDown() throws Exception {
    flinkCluster.after();
  }

  private static class Sink implements SinkFunction<Row> {
    public static final List<Row> values = Collections.synchronizedList(new ArrayList<>());
    @Override
    public void invoke(Row value, SinkFunction.Context context) throws Exception {
      values.add(value);
    }
  }
}

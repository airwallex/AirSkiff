package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.core.api.KStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.testhelpers.*;
import net.jqwik.api.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.jupiter.api.Assertions;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@PropertyDefaults(tries = 10)
public class StreamTest {
  private TestRunner runner() {
    return new TestRunner();
  }

  @Provide
  Arbitrary<Tuple2<Long, TestInputData>> flinkTuple() {
    // timestamp should not exceed 10000 years, as it will be allocated to another window
    // in our FlinkBatchCompiler
    return Arbitraries.longs()
      .between(1000000, (long) 3e14)
      .flatMap(l -> Arbitraries.integers().map(i -> new Tuple2<>(l, new TestInputData(i))));
  }

  @Property
  public void testMap(@ForAll List<@From(("flinkTuple")) Tuple2<Long, TestInputData>> data) throws Exception {
    if (data.isEmpty()) {
      return;
    }

    runner().executeAndCheck(source -> source.map(i -> new TestInputData(i.a * 2), TestInputData.class), data);
  }

  @Property
  public void testFlat(@ForAll List<@From(("flinkTuple")) Tuple2<Long, TestInputData>> data) throws Exception {
    if (data.isEmpty()) {
      return;
    }

    runner().executeAndCheck(source -> source.flatMap(i -> {
      ArrayList<TestInputData> list = new ArrayList<>();
      list.add(new TestInputData(i.a * 2));
      return list;
    }, TestInputData.class), data);
  }

  // Ignore the test for now
  // DataStream has non deterministic behavior: https://issues.apache.org/jira/browse/FLINK-21310
  public void testUnion(@ForAll List<@From(("flinkTuple")) Tuple2<Long, TestInputData>> data) throws Exception {
    // flink union is not stable when list is large
    if (data.isEmpty() || data.size() > 10) {
      return;
    }

    runner().executeAndCheck(source -> source.union(source).map(i -> new TestInputData(i.a * 2), TestInputData.class),
      data, true, false
    );
  }

  @Property
  public void testFilter(@ForAll List<@From(("flinkTuple")) Tuple2<Long, TestInputData>> data) throws Exception {
    if (data.isEmpty()) {
      return;
    }

    runner().executeAndCheck(source -> source.filter(i -> i.a % 2 == 0), data);
  }

  @Property
  public void testSum(@ForAll List<@From(("flinkTuple")) Tuple2<Long, TestInputData>> data) throws Exception {
    if (data.isEmpty()) {
      return;
    }

    var list = doubleList(data);
    list.sort(Comparator.comparing(o -> o.f0));
    runner().executeAndCheck(source -> source.keyBy(t -> t.b, String.class).sum(new TestMonoid()).values(),
      list,
      true,
      false
    );
  }

  @Property
  public void testOrderedSum(@ForAll List<@From(("flinkTuple")) Tuple2<Long, TestInputData>> data) throws Exception {
    if (data.isEmpty()) {
      return;
    }

    var list = doubleList(data);

    runner().executeAndCheck(
      source -> source.keyBy(t -> t.b, String.class).orderedSum(new TestMonoid(), TestInputData::compareTo).values(),
      list,
      true,
      true
    );
  }

  /**
   * This is a test that would fail with Kotlin
   */
  @Example
  public void testFlinkSqlApi() {
    TestRunner runner = runner();
    DataStream<Integer> d = runner.env.fromCollection(Arrays.asList(1, 2, 3));
    runner.tableEnv.createTemporaryView("abc", d);
    runner.tableEnv.sqlQuery("select * from abc");
  }

  @Property
  public void testSql(@ForAll List<@From(("flinkTuple")) Tuple2<Long, TestInputData>> data) throws Exception {
    if (data.isEmpty()) {
      return;
    }
    String tableName = "test_111";

    data =
      data.stream().map(t -> new Tuple2<>(t.f0 % 10000, new TestInputData(t.f1.a % 1000))).collect(Collectors.toList());

    runner().executeAndCheck(
      source -> source.map(t -> t, TestInputData.class).sql("SELECT CAST(b AS INT) as a, ABS(a) as b, CAST(a AS VARCHAR(64)) as c from " + tableName,
        tableName,
        TestData.class
      ),
      data
    );
  }

  @Property
  public void testLeftJoin(@ForAll List<@From(("flinkTuple")) Tuple2<Long, TestInputData>> data) throws Exception {
    if (data.isEmpty()) {
      return;
    }

    runner().executeAndCheck(source -> {
      KStream<String, TestInputData> orig = source.keyBy(d -> d.b, String.class);
      KStream<String, TestInputData> filtered = source.filter(i -> i.a % 2 == 0).keyBy(d -> d.b, String.class);
      KStream<String, Pair<TestInputData, TestInputData>> joined = orig.leftJoin(filtered);
      return joined.values().map(p -> new TestInputData(p.r == null ? -1 : p.r.a, p.l.b), TestInputData.class);
    }, data, true, true);
  }

  @Property
  public void testMapValue(@ForAll List<@From(("flinkTuple")) Tuple2<Long, TestInputData>> data) throws Exception {
    if (data.isEmpty()) {
      return;
    }

    runner().executeAndCheck(source -> source.keyBy(t -> t.b, String.class)
      .mapValue(t -> new TestInputData(t.a, t.b + t.b), TestInputData.class)
      .values(), data, true, false);
  }

  @Property
  public void testBatchLeftJoinWithDifferentTimestamps(
    @ForAll List<@From(("flinkTuple")) Tuple2<Long, TestInputData>> data
  ) throws Exception {
    if (data.isEmpty()) {
      return;
    }
    data.sort(Comparator.comparing(t -> t.f0));

    var config1 = new TestFlinkConfig<>(data, TestInputData.class);
    var source1 = new SourceStream<>(config1);

    List<Tuple2<Long, TestInputData>> data2 = new ArrayList<>();
    for (Tuple2<Long, TestInputData> t : data) {
      data2.add(new Tuple2<>(t.f0 - 100, t.f1));
    }
    var config2 = new TestFlinkConfig<>(data2, TestInputData.class);
    var source2 = new SourceStream<>(config2);

    LeftJoinStream<String, TestInputData, TestInputData> x =
      source1.keyBy(d -> d.b, String.class).leftJoin(source2.keyBy(d -> d.b, String.class));
      List<Tuple2<Long, Pair<String, Pair<TestInputData, TestInputData>>>> flinkBatchRes =
      runner().runFlinkBatch(x, data.size() * 2);

    Assertions.assertEquals(flinkBatchRes.size(), data.size());
    for (Tuple2<Long, Pair<String, Pair<TestInputData, TestInputData>>> t : flinkBatchRes) {
      Assertions.assertEquals(t.f1.r.l, t.f1.r.r);
    }
  }

  @Property
  public void testPreviousAndCurrent(@ForAll List<@From(("flinkTuple")) Tuple2<Long, TestInputData>> data)
    throws Exception {
    if (data.isEmpty()) {
      return;
    }
    data.sort(Comparator.comparing(t -> t.f0));

    var config = new TestFlinkConfig<>(data, TestInputData.class);
    var source = new SourceStream<>(config);
    KStream<String, TestInputData> orig = source.keyBy(d -> "1", String.class);
    Stream<TestInputData> previous =
      orig.previousAndCurrent().values().map(p -> (TestInputData) (p.l), TestInputData.class);
    Stream<TestInputData> current =
      orig.previousAndCurrent().values().map(p -> (TestInputData) (p.r), TestInputData.class);

    List<Tuple2<Long, TestInputData>> flinkRealtimePreviousRes = runner().runFlinkBatch(previous, data.size());
    List<Tuple2<Long, TestInputData>> flinkRealtimeCurrentRes = runner().runFlinkBatch(current, data.size());

    for (int i = 1; i < flinkRealtimeCurrentRes.size(); i++) {
      Assertions.assertEquals(flinkRealtimePreviousRes.get(i).f1, flinkRealtimeCurrentRes.get(i - 1).f1);
    }
  }

  @Property
  public void testWindow(@ForAll List<@From(("flinkTuple")) Tuple2<Long, TestInputData>> data)
    throws Exception {
    if (data.isEmpty()) {
      return;
    }
    runner().executeAndCheck(source -> source.keyBy(t -> t.b, String.class)
      .window(new EventTimeBasedSlidingWindow(Duration.ofSeconds(10), Duration.ofSeconds(5)), it -> {
        TestInputData last = null;
        for (TestInputData id : it) {
          last = id;
        }
        ArrayList<TestInputData> result = new ArrayList<>();
        result.add(last);
        return result;
        }, TestInputData::compareTo, TestInputData.class).values(), data, true, false);
  }

  private List<Tuple2<Long, TestInputData>> doubleList(List<Tuple2<Long, TestInputData>> data) {
    data.addAll(data);
    return data;
  }
}

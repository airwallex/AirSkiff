package com.airwallex.airskiff.flink;

import com.airwallex.airskiff.core.EventTimeBasedSlidingWindow;
import com.airwallex.airskiff.testhelpers.TestFlinkConfig;
import com.airwallex.airskiff.testhelpers.TestInputData;
import com.airwallex.airskiff.testhelpers.TestRunner;
import org.apache.flink.api.java.tuple.Tuple2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import com.airwallex.airskiff.core.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class FlinkWindowStreamTest {
  /*
  window allows max lateness is min(1 day, 14 * slide)
  when day 0 arrives, state keeps data of day 0
  when day 1 arrives, state keeps data of day 0, 1
  when day 4 arrives, state keeps data of day 4, delete data of day 0, 1
  when day 1 arrives again, state keeps data of day 4, 1
   */
  @Test
  public void testWindowSlide() throws Exception {
    List<Tuple2<Long, TestInputData>> data = new ArrayList<>();
    data.add(new Tuple2<>(0L, new TestInputData(1, "1")));
    data.add(new Tuple2<>(Duration.ofDays(1).toMillis(), new TestInputData(1, "1")));
    data.add(new Tuple2<>(Duration.ofDays(4).toMillis(), new TestInputData(1, "1")));
    data.add(new Tuple2<>(Duration.ofDays(1).toMillis(), new TestInputData(1, "1")));
    var config = new TestFlinkConfig<>(data, TestInputData.class);
    var source = new SourceStream<>(config);
    TestRunner runner = new TestRunner();
    var stream = runner.realtimeCompiler.compile(
      source.keyBy(t -> t.b, String.class)
        .window(new EventTimeBasedSlidingWindow(Duration.ofDays(1), Duration.ofDays(1)), it -> {
          // simple sum for TestInputData of last one day
          TestInputData aggregatedData = new TestInputData(0, "");
          for (TestInputData id : it) {
            aggregatedData.a += id.a;
            aggregatedData.b = id.b;
          }
          ArrayList<TestInputData> result = new ArrayList<>();
          result.add(aggregatedData);
          return result;
        }, TestInputData::compareTo, TestInputData.class).values()
    );
    var result = stream.executeAndCollect(4);
    assertEquals(4, result.size());
    assertEquals(1, result.get(0).f1.a);
    assertEquals(2, result.get(1).f1.a);
    assertEquals(1, result.get(2).f1.a);
    assertEquals(1, result.get(3).f1.a);
  }
}

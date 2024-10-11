package com.airwallex.airskiff;


import com.airwallex.airskiff.core.EventTimeBasedSlidingWindow;
import com.airwallex.airskiff.core.SourceStream;
import com.airwallex.airskiff.testhelpers.TestFlinkConfig;
import com.airwallex.airskiff.testhelpers.TestInputData;
import com.airwallex.airskiff.testhelpers.TestRunner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FlinkWindowPerformanceTest {

  private List<Tuple2<Long, TestInputData>> data;
  private static final int DATA_SIZE = 100000; // Increased for more substantial test
  private static final int NUM_KEYS = 1000;
  private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
  private static final Duration WINDOW_SLIDE = Duration.ofMinutes(1);
  private static final Duration TOTAL_TIME_SPAN = Duration.ofHours(1); // Total time span of the data

  @BeforeEach
  public void setUp() {
    data = generateData();
  }

  private List<Tuple2<Long, TestInputData>> generateData() {
    List<Tuple2<Long, TestInputData>> generatedData = new ArrayList<>(DATA_SIZE);
    Random random = new Random();
    long totalMillis = TOTAL_TIME_SPAN.toMillis();

    for (int i = 0; i < DATA_SIZE; i++) {
      // Generate timestamps within the TOTAL_TIME_SPAN
      long timestamp = random.nextLong() % totalMillis;
      if (timestamp < 0) timestamp += totalMillis;

      String key = "key" + (random.nextInt(NUM_KEYS) + 1);
      generatedData.add(new Tuple2<>(timestamp, new TestInputData(random.nextInt(100), key)));
    }

    // Sort the data by timestamp
    generatedData.sort((a, b) -> Long.compare(a.f0, b.f0));

    return generatedData;
  }

  private void runWindowTest(String version) throws Exception {
    TestRunner runner = new TestRunner();
    var config = new TestFlinkConfig<>(data, TestInputData.class);
    var source = new SourceStream<>(config);

    runner.realtimeCompiler.setWindowFnVersion(version);

    long startTime = System.nanoTime();

    var stream = runner.realtimeCompiler.compile(
      source.keyBy(t -> t.b, String.class)
        .window(new EventTimeBasedSlidingWindow(WINDOW_SIZE, WINDOW_SLIDE), it -> {
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

    var result = stream.executeAndCollect(DATA_SIZE);

    long endTime = System.nanoTime();
    long duration = (endTime - startTime) / 1_000_000; // Convert to milliseconds

    System.out.println("Window function " + version + " execution time: " + duration + " ms");
    System.out.println("Result size: " + result.size());
  }

  @Test
  public void testWindowPerformance() throws Exception {
    var rounds = 5;
    while (rounds > 0) {
      runWindowTest("v1");
      runWindowTest("v2");
      runWindowTest("v3");
      rounds--;
    }
  }
}

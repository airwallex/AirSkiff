package com.airwallex.airskiff.flink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 1. historical event means currentTime - eventTime > 10s 2. realtime event means currentTime -
 * eventTime <= 10s 3. continuous event means the interval of two event <= 10s 4. sparse event means
 * the interval of two event > 10s
 */
public class HybridWatermarkGeneratorTest {

  private final long MAX_DELAY = Duration.ofSeconds(1000).toMillis();
  private final long ALLOWED_DELAY = Duration.ofSeconds(1).toMillis();
  private List<Watermark> wms;
  private WatermarkOutput output;
  private EventTimeManager eventTimeManager;
  private HybridWatermarkGenerator<Integer> generator;
  private Instant instant;

  @BeforeEach
  public void init() {
    instant = Instant.ofEpochMilli(System.currentTimeMillis());
    Clock clock = new Clock() {
      @Override
      public ZoneId getZone() {
        return null;
      }

      @Override
      public Clock withZone(ZoneId zone) {
        return null;
      }

      @Override
      public Instant instant() {
        return instant;
      }
    };

    wms = new ArrayList<>();
    output = new WatermarkOutput() {
      @Override
      public void emitWatermark(Watermark watermark) {
        wms.add(watermark);
      }

      @Override
      public void markIdle() {
      }

      @Override
      public void markActive() {
      }
    };

    eventTimeManager = new EventTimeManager();
    generator = new HybridWatermarkGenerator<>(MAX_DELAY, eventTimeManager, clock);
  }

  @Test
  public void testHistoricalContinuousEvents() {
    long maxTs = 0;
    Random random = new Random();
    for (int i = 0; i < 3; i++) {
      long eventTime = instant.toEpochMilli() - Constants.TEN_SECONDS.toMillis() - random.nextInt(1000);
      maxTs = Math.max(maxTs, eventTime);
      generator.onEvent(i, eventTime, output);
      generator.onPeriodicEmit(output);
      Assertions.assertFalse(eventTimeManager.isCaughtUp());
      Assertions.assertEquals(maxTs - MAX_DELAY, wms.get(wms.size() - 1).getTimestamp());
    }
  }

  @Test
  public void testHistoricalSparseEvents() {
    Random random = new Random();
    for (int i = 0; i < 3; i++) {
      long eventTime = instant.toEpochMilli() - Constants.TEN_SECONDS.toMillis() - random.nextInt(1000);
      generator.onEvent(i, eventTime, output);
      // we have been idle more than ten seconds
      // emit watermark equal to the last historic event's timestamp
      instant = instant.plusMillis(Constants.TEN_SECONDS.toMillis());
      generator.onPeriodicEmit(output);
      Assertions.assertFalse(eventTimeManager.isCaughtUp());
      Assertions.assertEquals(eventTime, wms.get(wms.size() - 1).getTimestamp());
    }
  }

  @Test
  public void testRealtimeContinuousEvents() {
    long maxTs = 0;
    Random random = new Random();
    for (int i = 0; i < 3; i++) {
      long eventTime = instant.toEpochMilli() - random.nextInt(1000);
      maxTs = Math.max(maxTs, eventTime);
      generator.onEvent(i, eventTime, output);
      generator.onPeriodicEmit(output);
      Assertions.assertTrue(eventTimeManager.isCaughtUp());
      Assertions.assertEquals(maxTs, wms.get(wms.size() - 1).getTimestamp());
    }
  }

  @Test
  public void testRealtimeSparseEvents() {
    for (int i = 0; i < 3; i++) {
      long eventTime = instant.toEpochMilli() - new Random().nextInt(1000);
      generator.onEvent(i, eventTime, output);
      // we have been idle more than ten seconds
      instant = instant.plusMillis(Constants.TEN_SECONDS.toMillis());
      generator.onPeriodicEmit(output);
      Assertions.assertTrue(eventTimeManager.isCaughtUp());
      Assertions.assertEquals(eventTime, wms.get(wms.size() - 1).getTimestamp());
    }
  }

  @Test
  public void testRandomEvents() throws IOException {
    Path inputPath = Paths.get("src/test/resources/input.txt");
    Path outputPath = Paths.get("src/test/resources/output.txt");
    BufferedReader bufferedReaderOutput = Files.newBufferedReader(outputPath);

    List<Long> watermarks = generateWatermarks(inputPath);
    for (long watermark : watermarks) {
      String expected = bufferedReaderOutput.readLine();
      Assertions.assertEquals(Long.parseLong(expected), watermark);
    }
    bufferedReaderOutput.close();
  }

  @Test
  public void testAllowedLatency() {
    EventTimeManager lateEventTimeManager = new EventTimeManager();
    HybridWatermarkGenerator lateGenerator = new HybridWatermarkGenerator<>(MAX_DELAY, lateEventTimeManager, Clock.systemUTC(), ALLOWED_DELAY);
    for (int i = 0; i < 3; i++) {
      long eventTime = instant.toEpochMilli();
      lateGenerator.onEvent(i, eventTime, output);
      lateGenerator.onPeriodicEmit(output);
      Assertions.assertTrue(lateEventTimeManager.isCaughtUp());
      Assertions.assertEquals(eventTime - ALLOWED_DELAY, wms.get(wms.size() - 1).getTimestamp());
    }
  }

  @Disabled
  @Test
  public void outputWatermark() throws IOException {
    Path inputPath = Paths.get("src/test/resources/input.txt");
    Path outputPath = Paths.get("src/test/resources/output.txt");
    List<Long> watermarks = generateWatermarks(inputPath);
    writeToFile(watermarks, outputPath);
  }

  List<Long> generateWatermarks(Path inputPath) throws IOException {
    List<String> stringList = Files.readAllLines(inputPath);
    for (int i = 0; i < stringList.size() - 1; i++) {
      String[] s1 = stringList.get(i).split(",");
      long curClock = Long.parseLong(s1[0]);
      long curEventTime = Long.parseLong(s1[1]);

      String[] s2 = stringList.get(i + 1).split(",");
      long nextClock = Long.parseLong(s2[0]);

      generator.onEvent(0, curEventTime, output);
      // interval=1s
      for (long j = curClock; j < nextClock; j += 1000) {
        instant = Instant.ofEpochMilli(j);
        generator.onPeriodicEmit(output);
      }
    }
    List<Long> result = new ArrayList<>();
    for (Watermark watermark : wms) {
      result.add(watermark.getTimestamp());
    }
    return result;
  }

  void writeToFile(List<Long> watermarks, Path outputPath) throws IOException {
    BufferedWriter bufferedWriter = Files.newBufferedWriter(outputPath);
    for (Long watermark : watermarks) {
      bufferedWriter.write("" + watermark);
      bufferedWriter.newLine();
    }
    bufferedWriter.close();
  }

  @Disabled
  @Test
  public void generateRandomInputEvents() throws IOException {
    Path path = Paths.get("src/test/resources/input.txt");
    BufferedWriter bufferedWriter = Files.newBufferedWriter(path);
    long clock = 1609459100000L;
    long eventTime;
    Random random = new Random();
    for (int i = 0; i < 50; i++) {
      clock += random.nextInt(15) * 1000;
      eventTime = clock - (random.nextInt(60) - 10) * 1000;
      bufferedWriter.write(clock + "," + eventTime);
      bufferedWriter.newLine();
    }
    bufferedWriter.close();
  }
}

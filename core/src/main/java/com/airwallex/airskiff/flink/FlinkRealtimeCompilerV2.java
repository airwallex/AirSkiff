package com.airwallex.airskiff.flink;

import com.airwallex.airskiff.common.Pair;
// No longer need LeftJoinPairMonoid
import com.airwallex.airskiff.core.LeftJoinStream;
import com.airwallex.airskiff.flink.types.PairTypeInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger; // Added Logger
import org.slf4j.LoggerFactory; // Added Logger Factory

import java.time.Duration;

import static com.airwallex.airskiff.flink.Utils.pairType;
import static com.airwallex.airskiff.flink.Utils.typeInfo;

/**
 * Final V2: Corrected state update logic in LeftJoinProcessFunction.
 * Includes timer management with loosened onTimer check. Addresses data loss
 * and duplicate issues in left joins during catch-up.
 */
public class FlinkRealtimeCompilerV2 extends AbstractFlinkCompiler {

  public FlinkRealtimeCompilerV2(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, Duration allowedLatency,
                                 Duration withIdleness) {
    super(env, tableEnv, allowedLatency, withIdleness);
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
  }

  @Override
  protected <K, T, U> KeyedStream<Tuple2<Long, Pair<K, Pair<T, U>>>, K> compileLeftJoin(
    LeftJoinStream<K, T, U> stream
  ) {
    KeyedStream<Tuple2<Long, Pair<K, T>>, K> ks1 = compileKS(stream.s1);
    KeyedStream<Tuple2<Long, Pair<K, U>>, K> ks2 = compileKS(stream.s2);
    final TypeInformation<K> kType = typeInfo(stream.keyClass());
    final TypeInformation<Pair<T, U>> pairType = pairType(stream);
    final TypeInformation<Pair<K, Pair<T, U>>> keyedPairType = new PairTypeInfo<>(kType, pairType);
    final TypeInformation<Tuple2<Long, Pair<K, Pair<T, U>>>> outputType =
      new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, keyedPairType);

    // Union streams mapping left to Pair(T, null) and right to Pair(null, U)
    DataStream<Tuple2<Long, Pair<K, Pair<T, U>>>> ss =
      ks2.map(x -> new Tuple2<>(x.f0, new Pair<>(x.f1.l, new Pair<T, U>(null, x.f1.r))), outputType)
        .union(ks1.map(x -> new Tuple2<>(x.f0, new Pair<>(x.f1.l, new Pair<T, U>(x.f1.r, null))), outputType));

    final boolean batchMode = isBatch();
    final Duration currentAllowedLatency = this.allowedLatency;

    return ss
      .keyBy(t -> t.f1.l, kType)
      .process(new LeftJoinProcessFunction<>(pairType, currentAllowedLatency, batchMode), outputType)
      .keyBy(t -> t.f1.l, kType);
  }

  @Override
  protected boolean isBatch() {
    return false;
  }

  /**
   * Handles left join logic using state and timers. Corrected state update logic.
   */
  public static class LeftJoinProcessFunction<K, T, U>
    extends KeyedProcessFunction<K, Tuple2<Long, Pair<K, Pair<T, U>>>, Tuple2<Long, Pair<K, Pair<T, U>>>> {

    private static final Logger log = LoggerFactory.getLogger(LeftJoinProcessFunction.class); // Added Logger

    // Monoid 'm' removed
    private final TypeInformation<Pair<T, U>> pairTypeInfo;
    private final Duration allowedLatencyDuration;
    private final boolean isBatchMode;
    private static final int SEVEN_DAYS_IN_SECONDS = 7 * 24 * 60 * 60;
    private static final int ONE_DAY_IN_SECONDS = 24 * 60 * 60;

    private transient ValueState<Pair<T, U>> state;
    private transient ValueState<Long> activeTimerTimestampState;

    public LeftJoinProcessFunction(
      TypeInformation<Pair<T, U>> pairTypeInfo,
      Duration allowedLatencyDuration,
      boolean isBatchMode) {
      this.pairTypeInfo = pairTypeInfo;
      this.allowedLatencyDuration = allowedLatencyDuration != null ? allowedLatencyDuration : Duration.ZERO;
      this.isBatchMode = isBatchMode;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      StateTtlConfig ttlConfig = StateTtlConfig
        .newBuilder(Time.seconds(isBatchMode ? SEVEN_DAYS_IN_SECONDS : ONE_DAY_IN_SECONDS))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .cleanupIncrementally(10, false)
        .build();
      ValueStateDescriptor<Pair<T, U>> pairStateDescriptor = new ValueStateDescriptor<>("pairState", pairTypeInfo);
      pairStateDescriptor.enableTimeToLive(ttlConfig);
      state = getRuntimeContext().getState(pairStateDescriptor);
      ValueStateDescriptor<Long> timerStateDescriptor = new ValueStateDescriptor<>("activeTimerTs", Long.class);
      activeTimerTimestampState = getRuntimeContext().getState(timerStateDescriptor);
      log.info("Opened LeftJoinProcessFunction with allowedLatency: {}, isBatchMode: {}", allowedLatencyDuration, isBatchMode);
    }

    @Override
    public void processElement(
      Tuple2<Long, Pair<K, Pair<T, U>>> inputTuple, Context ctx, Collector<Tuple2<Long, Pair<K, Pair<T, U>>>> out)
      throws Exception {

      K currentKey = ctx.getCurrentKey();
      Pair<T, U> currentState = state.value();
      Pair<T, U> incomingUpdate = inputTuple.f1.r; // This is Pair(T_in, null) or Pair(null, U_in)

      T currentT = (currentState == null) ? null : currentState.l;
      U currentU = (currentState == null) ? null : currentState.r;

      T incomingT = incomingUpdate.l;
      U incomingU = incomingUpdate.r;

      // *** Corrected State Update Logic ***
      T nextT = (incomingT != null) ? incomingT : currentT;
      U nextU = (incomingU != null) ? incomingU : currentU;

      Pair<T, U> nextState = new Pair<>(nextT, nextU);
      state.update(nextState);
      // *** End Corrected State Update Logic ***


      // --- Timer Management Logic ---
      long newTimerTime;
      if (isBatchMode) {
        newTimerTime = inputTuple.f0 + (long) SEVEN_DAYS_IN_SECONDS * 1000L;
      } else {
        // Timer based on event time + allowed latency
        // Add 1ms to ensure watermark passes it even if latency is 0
        newTimerTime = inputTuple.f0 + allowedLatencyDuration.toMillis() + 1;
      }
      Long currentTimerTimestamp = activeTimerTimestampState.value();
      if (currentTimerTimestamp != null) {
        ctx.timerService().deleteEventTimeTimer(currentTimerTimestamp);
      }
      ctx.timerService().registerEventTimeTimer(newTimerTime);
      activeTimerTimestampState.update(newTimerTime);
      // --- End Timer Management Logic ---
    }

    @Override
    public void onTimer(
      long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, Pair<K, Pair<T, U>>>> out)
      throws Exception {
      K currentKey = ctx.getCurrentKey();
      Long expectedTimerTimestamp = activeTimerTimestampState.value();

      // Loosened Check: Only proceed if a timer was expected (state is not null).
      // Ignore timers that might fire after deletion due to race conditions.
      if (expectedTimerTimestamp == null) {
        return;
      }

      // Optimization: Can optionally add check `timestamp < expectedTimerTimestamp` to ignore late-firing old timers
      // if deleteEventTimeTimer is not perfectly reliable in all edge cases. But the null check is primary.
      // if (timestamp < expectedTimerTimestamp) { return; }

      // Clear the stored timestamp state as this timer is now processed.
      activeTimerTimestampState.clear();

      // Get the current state *at the time the timer fired*.
      Pair<T, U> currentState = state.value();

      // Emit the state *only if* the left side exists when the timer fires.
      if (currentState != null && currentState.l != null) {
        out.collect(new Tuple2<>(timestamp, new Pair<>(currentKey, currentState)));
      } else {
        log.debug("Not emitting for key: {} at {} because left side is null in state: {}", currentKey, timestamp, currentState);
      }
    }
  }
}

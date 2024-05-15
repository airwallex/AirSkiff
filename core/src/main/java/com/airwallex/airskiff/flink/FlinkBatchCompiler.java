package com.airwallex.airskiff.flink;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.core.OrderedSummedStream;
import com.airwallex.airskiff.core.StreamUtils;
import com.airwallex.airskiff.flink.types.PairTypeInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.airwallex.airskiff.flink.Utils.typeInfo;

/**
 * A Compiler that compiles Stream into Flink DataStream for batch processing TODO: use memomization
 * to optimize diamonds in a DAG
 */
public class FlinkBatchCompiler extends AbstractFlinkCompiler {
  public static final TumblingEventTimeWindows WINDOW = TumblingEventTimeWindows.of(Time.days(365 * 10000));

  public FlinkBatchCompiler(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
    super(env, tableEnv, Duration.ZERO, Duration.ofMillis(300));
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
  }

  @Override
  protected <K, T> KeyedStream<Tuple2<Long, Pair<K, T>>, K> compileOrderedSum(
    OrderedSummedStream<K, T> stream
  ) {
    KeyedStream<Tuple2<Long, Pair<K, T>>, K> ks = compileKS(stream.stream);
    return new KeyedStream<>(ks.window(WINDOW)
      .process(
        new ProcessWindowFunction<Tuple2<Long, Pair<K, T>>, Tuple2<Long, Pair<K, T>>, K, TimeWindow>() {
          @Override
          public void process(
            K k, Context context, Iterable<Tuple2<Long, Pair<K, T>>> iterable, Collector<Tuple2<Long, Pair<K, T>>> collector
          ) throws Exception {
            List<Tuple2<Long, Pair<K, T>>> l = new ArrayList<>();
            for (Tuple2<Long, Pair<K, T>> e : iterable) {
              l.add(e);
            }
            l.sort((o1, o2) -> {
              int res = o1.f0.compareTo(o2.f0);
              if (res == 0) {
                return stream.order.compare(o1.f1.r, o2.f1.r);
              }
              return res;
            });
            T total = stream.monoid.zero();
            for (Tuple2<Long, Pair<K, T>> e : l) {
              total = stream.monoid.plus(total, e.f1.r);
              collector.collect(new Tuple2<>(e.f0, new Pair<>(e.f1.l, total)));
            }
          }
        },
        new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO,
          new PairTypeInfo<>(typeInfo(stream.keyClass()), typeInfo(StreamUtils.kStreamClass(stream)))
        )
      ), t -> t.f1.l);
  }

  @Override
  protected boolean isBatch() {
    return true;
  }
}

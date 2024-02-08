package com.airwallex.airskiff.flink;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.core.LeftJoinPairMonoid;
import com.airwallex.airskiff.core.LeftJoinStream;
import com.airwallex.airskiff.flink.types.PairTypeInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

import java.time.Duration;

import static com.airwallex.airskiff.flink.Utils.pairType;
import static com.airwallex.airskiff.flink.Utils.typeInfo;

public class FlinkRealtimeCompiler extends AbstractFlinkCompiler {

  public FlinkRealtimeCompiler(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, Duration allowedLatency) {
    super(env, tableEnv, allowedLatency);
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

    final LeftJoinPairMonoid<T, U> m = new LeftJoinPairMonoid<>();
    DataStream<Tuple2<Long, Pair<K, Pair<T, U>>>> ss =
      ks2.map(x -> new Tuple2<>(x.f0, new Pair<>(x.f1.l, new Pair<T, U>(null, x.f1.r))), outputType)
        .union(ks1.map(x -> new Tuple2<>(x.f0, new Pair<>(x.f1.l, new Pair<T, U>(x.f1.r, null))), outputType));
    return ss
      .keyBy(t -> t.f1.l, kType)
      .process(new KeyedProcessFunction<>() {
        // This is a global state per key
        private ValueState<Pair<T, U>> state;

        @Override
        public void open(Configuration parameters) throws Exception {
          state = getRuntimeContext().getState(new ValueStateDescriptor<>("pairState", pairType));
        }

        @Override
        public void processElement(
          Tuple2<Long, Pair<K, Pair<T, U>>> t, Context ctx, Collector<Tuple2<Long, Pair<K, Pair<T, U>>>> out
        ) throws Exception {
          Pair<T, U> cur = state.value();
          cur = m.plus(cur, t.f1.r);
          state.update(cur);
          out.collect(new Tuple2<>(t.f0, new Pair<>(t.f1.l, cur)));
        }
      }, outputType)
      .filter(t -> t.f1.r.l != null)
      .keyBy(t -> t.f1.l, kType);
  }

  @Override
  protected boolean isBatch() {
    return false;
  }
}

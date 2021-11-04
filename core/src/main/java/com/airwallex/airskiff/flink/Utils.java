package com.airwallex.airskiff.flink;

import com.airwallex.airskiff.core.LeftJoinStream;
import com.airwallex.airskiff.core.StreamUtils;
import com.airwallex.airskiff.flink.types.PairTypeInfo;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class Utils {

  public static <T> WatermarkStrategy<Tuple2<Long, T>> watermark(boolean isBatch) {
    if (isBatch) {
      return WatermarkStrategy.<Tuple2<Long, T>>forBoundedOutOfOrderness(Duration.ofDays(
        365 * 10000)).withTimestampAssigner((t, l) -> t.f0);
    } else {
      return WatermarkStrategy.<Tuple2<Long, T>>forBoundedOutOfOrderness(Duration.ofMillis(100)).withTimestampAssigner((t, l) -> t.f0);
    }
  }


  public static <T> TypeInformation<T> typeInfo(Class<T> tc) {
    return TypeExtractor.createTypeInfo(tc);
  }

  public static <T> TypeInformation<Tuple2<Long, T>> tuple2TypeInfo(Class<T> tc) {
    return new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, typeInfo(tc));
  }

  public static <L, R> PairTypeInfo<L, R> pairType(LeftJoinStream<?, L, R> ljs) {
    TypeInformation<?> lt;
    TypeInformation<?> rt;
    if (ljs.s1 instanceof LeftJoinStream) {
      lt = pairType((LeftJoinStream<?, ?, ?>) ljs.s1);
    } else {
      lt = typeInfo(StreamUtils.clz(ljs.s1));
    }
    if (ljs.s2 instanceof LeftJoinStream) {
      rt = pairType((LeftJoinStream<?, ?, ?>) ljs.s2);
    } else {
      rt = typeInfo(StreamUtils.clz(ljs.s2));
    }
    return new PairTypeInfo(lt, rt);
  }
}

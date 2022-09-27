package com.airwallex.airskiff.flink;

import com.airwallex.airskiff.core.LeftJoinStream;
import com.airwallex.airskiff.core.StreamUtils;
import com.airwallex.airskiff.flink.types.AvroGenericRecordConverter;
import com.airwallex.airskiff.flink.types.PairTypeInfo;
import com.airwallex.airskiff.flink.udx.GetAgeFunction;
import com.airwallex.airskiff.flink.udx.NormalizeNameFunction;
import com.airwallex.airskiff.flink.udx.UnixTimeFunction;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Utils {

  public static <T> WatermarkStrategy<Tuple2<Long, T>> watermark(boolean isBatch) {
    if (isBatch) {
      return WatermarkStrategy.<Tuple2<Long, T>>forBoundedOutOfOrderness(Duration.ofDays(365 * 10000))
        .withTimestampAssigner((t, l) -> t.f0);
    } else {
      return new RealtimeWatermarkStrategy<Tuple2<Long, T>>(Duration.ofDays(5)).withTimestampAssigner((t, l) -> t.f0).withIdleness(Duration.ofMillis(300));
    }
  }

  public static void registerFunctions(StreamTableEnvironment env) {
    Set<String> funcs = new HashSet<>(Arrays.asList(env.listFunctions()));
    if (!funcs.contains(GetAgeFunction.name.toLowerCase())) {
      env.createTemporarySystemFunction(GetAgeFunction.name, new GetAgeFunction());
    }
    if (!funcs.contains(NormalizeNameFunction.name.toLowerCase())) {
      env.createTemporarySystemFunction(NormalizeNameFunction.name, new NormalizeNameFunction());
    }
    if (!funcs.contains(UnixTimeFunction.name.toLowerCase())) {
      env.createTemporarySystemFunction(UnixTimeFunction.name, new UnixTimeFunction());
    }
  }

  public static <T> T convertRow(final Row row, final Class<T> uc) {
    if (SpecificRecordBase.class.isAssignableFrom(uc)) {
      return AvroGenericRecordConverter.convertToSpecificRecord(uc, row);
    } else {
      Field[] fields = uc.getFields();
      // r contains an extra ts field;
      assert (row.getArity() == fields.length + 1);
      Object[] args = new Object[fields.length];
      Class<?>[] types = new Class<?>[fields.length];
      for (int i = 0; i < fields.length; i++) {
        args[i] = row.getField(i + 1);
        types[i] = fields[i].getType();
      }
      try {
        return uc.getDeclaredConstructor(types).newInstance(args);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static <T> TypeInformation<T> typeInfo(Class<T> tc) {
    return TypeExtractor.createTypeInfo(tc);
  }

  public static <T> TypeInformation<Tuple2<Long, T>> tuple2TypeInfo(Class<T> tc) {
    return new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, typeInfo(tc));
  }

  public static TypeInformation<Tuple2<Long, GenericRecord>> tuple2TypeInfo(Schema schema) {
    return new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, new GenericRecordAvroTypeInfo(schema));
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

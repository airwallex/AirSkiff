package com.airwallex.airskiff.core;

import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.flink.Utils;
import com.airwallex.airskiff.flink.types.AvroGenericRecordConverter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.lang.reflect.Field;

public class SqlStream<T, U> implements Stream<U> {
  public final Stream<T> stream;
  public final String sql;
  public final String tableName;
  // map from table row to output type
  public final MapFunction<Row, U> mapper;
  public final TypeInformation<Tuple2<Long, U>> typeInfo;
  public final Class<U> tc;

  public SqlStream(Stream<T> stream, String sql, String tableName, Class<U> tc) {
    this(stream, sql, tableName, tc, r -> convertRow(r, tc), Utils.tuple2TypeInfo(tc));
  }

  public SqlStream(
    Stream<T> stream,
    String sql,
    String tableName,
    Class<U> tc,
    MapFunction<Row, U> func,
    TypeInformation<Tuple2<Long, U>> typeInfo
  ) {
    this.sql = sql;
    if (!this.sql.substring(0, 6).toLowerCase().startsWith("select")) {
      throw new IllegalArgumentException(String.format("SQL query must start with a SELECT statement. [%s]", this.sql));
    }
    this.stream = stream;
    this.tableName = tableName;
    this.mapper = func;
    this.typeInfo = typeInfo;
    this.tc = tc;
  }

  private static <T> T convertRow(final Row row, final Class<T> uc) {
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

  @Override
  public Class<U> getClazz() {
    return tc;
  }
}

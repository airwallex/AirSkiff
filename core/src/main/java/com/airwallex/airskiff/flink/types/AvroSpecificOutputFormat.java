package com.airwallex.airskiff.flink.types;

import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.Serializable;

public class AvroSpecificOutputFormat<E extends SpecificRecordBase> extends FileOutputFormat<E>
  implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String userDefinedSchema;
  private transient DataFileWriter<E> dataFileWriter;
  private int numTasks = 0;

  public AvroSpecificOutputFormat(Path filePath, Schema schema) {
    super(filePath);
    userDefinedSchema = schema.toString();
  }

  protected String getDirectoryFileName(int taskNumber) {
    return super.getDirectoryFileName(taskNumber) + ".avro";
  }

  @Override
  public void writeRecord(E record) throws IOException {
    this.dataFileWriter.append(record);
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    super.open(taskNumber, numTasks);
    this.numTasks = numTasks;

    Schema schema = new Schema.Parser().parse(userDefinedSchema);
    SpecificDatumWriter<E> datumWriter = new SpecificDatumWriter<>(schema);
    // This is a work around for a bug in Avro, which would ignore logicalType if it
    // is in a union, such as
    // {"name":"created_at","type":["null",{"type":"long","logicalType":"timestamp-micros"}]}
    datumWriter.getSpecificData().addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, this.stream);
  }

  @Override
  public void close() throws IOException {
    this.dataFileWriter.flush();
    this.dataFileWriter.close();
    if (numTasks > 1) {
      // write _SUCCESS file for jobs with numTasks > 1, which will have a directory
      // to store outputs
      FileSystem fs = outputFilePath.getFileSystem();
      fs.create(outputFilePath.suffix("/_SUCCESS"), FileSystem.WriteMode.OVERWRITE);
    }
    super.close();
  }
}

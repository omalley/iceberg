/*
 * Copyright 2018 Hortonworks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */package com.netflix.iceberg.orc;

import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.io.OutputFile;
import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Create a file appender for ORC.
 */
public class OrcFileAppender implements FileAppender<VectorizedRowBatch> {
  private final Writer writer;
  private final TypeDescription schema;

  public static final String COLUMN_NUMBERS_ATTRIBUTE = "iceberg.column.ids";

  static ByteBuffer buidIdString(List<Integer> list) {
    StringBuilder buffer = new StringBuilder();
    for(int i=0; i < list.size(); ++i) {
      if (i != 0) {
        buffer.append(',');
      }
      buffer.append(list.get(i));
    }
    return ByteBuffer.wrap(buffer.toString().getBytes(StandardCharsets.UTF_8));
  }

  OrcFileAppender(PartitionSpec spec,
                  OutputFile file,
                  OrcFile.WriterOptions options,
                  Map<String,byte[]> metadata) {
    List<Integer> columnIds = new ArrayList<>();
    schema = TypeConversion.toOrc(spec.schema(), columnIds);
    options.setSchema(schema);
    try {
      writer = OrcFile.createWriter(new Path(file.location()), options);
    } catch (IOException e) {
      throw new RuntimeException("Can't create file " + file.location(), e);
    }
    writer.addUserMetadata(COLUMN_NUMBERS_ATTRIBUTE, buidIdString(columnIds));
    metadata.forEach(
        (key,value) -> writer.addUserMetadata(key, ByteBuffer.wrap(value)));
  }

  @Override
  public void add(VectorizedRowBatch datum) {
    try {
      writer.addRowBatch(datum);
    } catch (IOException e) {
      throw new RuntimeException("Problem writing to ORC file", e);
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  public TypeDescription getSchema() {
    return schema;
  }
}

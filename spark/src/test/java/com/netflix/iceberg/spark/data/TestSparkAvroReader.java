/*
 * Copyright 2017 Netflix, Inc.
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
 */

package com.netflix.iceberg.spark.data;

import com.google.common.collect.Lists;
import com.netflix.iceberg.Files;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.avro.AvroIterable;
import com.netflix.iceberg.io.FileAppender;
import org.apache.avro.generic.GenericData.Record;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.netflix.iceberg.spark.data.TestHelpers.assertEqualsUnsafe;

public class TestSparkAvroReader extends AvroDataTest {
  protected void writeAndValidate(Schema schema) throws IOException {
    List<Record> expected = RandomData.generate(schema, 100, 0L);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer = Avro.write(Files.localOutput(testFile))
        .schema(schema)
        .named("test")
        .build()) {
      for (Record rec : expected) {
        writer.add(rec);
      }
    }

    List<InternalRow> rows;
    try (AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
        .createReaderFunc(SparkAvroReader::new)
        .project(schema)
        .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      assertEqualsUnsafe(schema.asStruct(), expected.get(i), rows.get(i));
    }
  }
}

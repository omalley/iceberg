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

import org.apache.hadoop.fs.Path;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * An adaptor so that the ORC RecordReader can be used as an Iterable.
 * The iterator() method can only be called once, because the iterator
 * stores its state in this object.
 */
public class OrcIterable implements Iterable<VectorizedRowBatch>, Closeable {
  private final Path filename;
  private final RecordReader rows;
  private final VectorizedRowBatch batch;
  private boolean advanced = false;

  OrcIterable(Path filename, TypeDescription schema, RecordReader rows) {
    this.filename = filename;
    this.rows = rows;
    this.batch = schema.createRowBatch();
  }

  @Override
  public void close() throws IOException {
    rows.close();
  }

  @Override
  public Iterator<VectorizedRowBatch> iterator() {
    return new Iterator<VectorizedRowBatch>() {

      private void advance() {
        if (!advanced) {
          try {
            rows.nextBatch(batch);
          } catch (IOException e) {
            throw new RuntimeException("Problem reading ORC file " + filename, e);
          }
          advanced = true;
        }
      }

      @Override
      public boolean hasNext() {
        advance();
        return batch.size > 0;
      }

      @Override
      public VectorizedRowBatch next() {
        // make sure we have the next batch
        advance();
        // mark it as used
        advanced = false;
        return batch;
      }
    };
  }
}

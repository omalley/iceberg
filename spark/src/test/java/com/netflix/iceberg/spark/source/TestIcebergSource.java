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

package com.netflix.iceberg.spark.source;

import com.netflix.iceberg.Table;
import org.apache.spark.sql.sources.v2.DataSourceV2Options;

public class TestIcebergSource extends IcebergSource {
  @Override
  public String shortName() {
    return "iceberg-test";
  }

  @Override
  protected Table findTable(DataSourceV2Options options) {
    return TestTables.load(options.get("iceberg.table.name").get());
  }
}

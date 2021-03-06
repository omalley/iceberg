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

package com.netflix.iceberg.avro;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.TypeUtil;
import com.netflix.iceberg.types.Types;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import java.util.List;
import java.util.Map;

import static com.netflix.iceberg.avro.AvroSchemaUtil.ADJUST_TO_UTC_PROP;
import static com.netflix.iceberg.avro.AvroSchemaUtil.ELEMENT_ID_PROP;
import static com.netflix.iceberg.avro.AvroSchemaUtil.FIELD_ID_PROP;
import static com.netflix.iceberg.avro.AvroSchemaUtil.KEY_ID_PROP;
import static com.netflix.iceberg.avro.AvroSchemaUtil.VALUE_ID_PROP;
import static com.netflix.iceberg.avro.AvroSchemaUtil.toOption;

class TypeToSchema extends TypeUtil.SchemaVisitor<Schema> {
  private static final Schema BOOLEAN_SCHEMA = Schema.create(Schema.Type.BOOLEAN);
  private static final Schema INTEGER_SCHEMA = Schema.create(Schema.Type.INT);
  private static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);
  private static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
  private static final Schema DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE);
  private static final Schema DATE_SCHEMA = LogicalTypes.date()
      .addToSchema(Schema.create(Schema.Type.INT));
  private static final Schema TIME_SCHEMA = LogicalTypes.timeMicros()
      .addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema TIMETZ_SCHEMA = LogicalTypes.timeMicros()
      .addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema TIMESTAMP_SCHEMA = LogicalTypes.timestampMicros()
      .addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema TIMESTAMPTZ_SCHEMA = LogicalTypes.timestampMicros()
      .addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private static final Schema UUID_SCHEMA = LogicalTypes.uuid()
      .addToSchema(Schema.createFixed("uuid_fixed", null, null, 16));
  private static final Schema BINARY_SCHEMA = Schema.create(Schema.Type.BYTES);

  static {
    TIME_SCHEMA.addProp(ADJUST_TO_UTC_PROP, false);
    TIMESTAMP_SCHEMA.addProp(ADJUST_TO_UTC_PROP, false);
    TIMETZ_SCHEMA.addProp(ADJUST_TO_UTC_PROP, true);
    TIMESTAMPTZ_SCHEMA.addProp(ADJUST_TO_UTC_PROP, true);
  }

  private final Map<Type, Schema> results = Maps.newHashMap();
  private final Map<Types.StructType, String> names;

  TypeToSchema(Map<Types.StructType, String> names) {
    this.names = names;
  }

  Map<Type, Schema> getConversionMap() {
    return results;
  }

  @Override
  public Schema schema(com.netflix.iceberg.Schema schema, Schema structSchema) {
    return structSchema;
  }

  @Override
  public Schema struct(Types.StructType struct, List<Schema> fieldSchemas) {
    Schema recordSchema = results.get(struct);
    if (recordSchema != null) {
      return recordSchema;
    }

    String recordName = names.get(struct);
    if (recordName == null) {
      recordName = fieldNames.peek();
    }

    List<Types.NestedField> structFields = struct.fields();
    List<Schema.Field> fields = Lists.newArrayListWithExpectedSize(fieldSchemas.size());
    for (int i = 0; i < structFields.size(); i += 1) {
      Types.NestedField structField = structFields.get(i);
      Schema.Field field = new Schema.Field(
          structField.name(), fieldSchemas.get(i), null,
          structField.isOptional() ? JsonProperties.NULL_VALUE : null);
      field.addProp(FIELD_ID_PROP, structField.fieldId());
      fields.add(field);
    }

    recordSchema = Schema.createRecord(recordName, null, null, false, fields);

    results.put(struct, recordSchema);

    return recordSchema;
  }

  @Override
  public Schema field(Types.NestedField field, Schema fieldSchema) {
    if (field.isOptional()) {
      return toOption(fieldSchema);
    } else {
      return fieldSchema;
    }
  }

  @Override
  public Schema list(Types.ListType list, Schema elementSchema) {
    Schema listSchema = results.get(list);
    if (listSchema != null) {
      return listSchema;
    }

    if (list.isElementOptional()) {
      listSchema = Schema.createArray(toOption(elementSchema));
    } else {
      listSchema = Schema.createArray(elementSchema);
    }

    listSchema.addProp(ELEMENT_ID_PROP, list.elementId());

    results.put(list, listSchema);

    return listSchema;
  }

  @Override
  public Schema map(Types.MapType map, Schema valueSchema) {
    Schema mapSchema = results.get(map);
    if (mapSchema != null) {
      return mapSchema;
    }

    if (map.isValueOptional()) {
      mapSchema = Schema.createMap(toOption(valueSchema));
    } else {
      mapSchema = Schema.createMap(valueSchema);
    }

    mapSchema.addProp(KEY_ID_PROP, map.keyId());
    mapSchema.addProp(VALUE_ID_PROP, map.valueId());

    results.put(map, mapSchema);

    return mapSchema;
  }

  @Override
  public Schema primitive(Type.PrimitiveType primitive) {
    Schema primitiveSchema;
    switch (primitive.typeId()) {
      case BOOLEAN:
        primitiveSchema = BOOLEAN_SCHEMA;
        break;
      case INTEGER:
        primitiveSchema = INTEGER_SCHEMA;
        break;
      case LONG:
        primitiveSchema = LONG_SCHEMA;
        break;
      case FLOAT:
        primitiveSchema = FLOAT_SCHEMA;
        break;
      case DOUBLE:
        primitiveSchema = DOUBLE_SCHEMA;
        break;
      case DATE:
        primitiveSchema = DATE_SCHEMA;
        break;
      case TIME:
        if (((Types.TimeType) primitive).shouldAdjustToUTC()) {
          primitiveSchema = TIMETZ_SCHEMA;
        } else {
          primitiveSchema = TIME_SCHEMA;
        }
        break;
      case TIMESTAMP:
        if (((Types.TimestampType) primitive).shouldAdjustToUTC()) {
          primitiveSchema = TIMESTAMPTZ_SCHEMA;
        } else {
          primitiveSchema = TIMESTAMP_SCHEMA;
        }
        break;
      case STRING:
        primitiveSchema = STRING_SCHEMA;
        break;
      case UUID:
        primitiveSchema = UUID_SCHEMA;
        break;
      case FIXED:
        Types.FixedType fixed = (Types.FixedType) primitive;
        primitiveSchema = Schema.createFixed("fixed_" + fixed.length(), null, null, fixed.length());
        break;
      case BINARY:
        primitiveSchema = BINARY_SCHEMA;
        break;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) primitive;
        primitiveSchema = LogicalTypes.decimal(decimal.precision(), decimal.scale())
            .addToSchema(Schema.createFixed(
                "decimal_" + decimal.precision() + "_" + decimal.scale(),
                null, null, TypeUtil.decimalRequriedBytes(decimal.precision())));
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported type ID: " + primitive.typeId());
    }

    results.put(primitive, primitiveSchema);

    return primitiveSchema;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.module.hive.udf.generic;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Extend Hive's UDFToString to support cast list to string .*/
public class HiveGenericUDFToString extends GenericUDF {

    private static final Logger LOG = LoggerFactory.getLogger(HiveGenericUDFToString.class.getName());

    public static final String UDF_NAME = "UDFToString";
    public static final String NAME = "string";

    private transient ObjectInspector argumentOI;
    private transient TextConverter converter;

    public HiveGenericUDFToString() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("STRING cast requires a value argument");
        }

        argumentOI = arguments[0];

        converter = new TextConverter(argumentOI);
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o0 = arguments[0].get();
        if (o0 == null) {
            return null;
        }

        return converter.convert(o0);
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        StringBuilder sb = new StringBuilder();
        sb.append("CAST( ");
        sb.append(children[0]);
        sb.append(" AS STRING)");
        return sb.toString();
    }


    public static class TextConverter implements ObjectInspectorConverters.Converter {
        private final ObjectInspector inputOI;
        private final Text t = new Text();
        private final ByteStream.Output out = new ByteStream.Output();
        private static byte[] trueBytes = new byte[]{84, 82, 85, 69};
        private static byte[] falseBytes = new byte[]{70, 65, 76, 83, 69};

        public TextConverter(ObjectInspector inputOI) {
            this.inputOI = inputOI;
        }

        public Text convert(Object input) {
            if (input == null) {
                return null;
            } else {
                if (this.inputOI instanceof ListObjectInspector) {
                    StringBuilder builder = new StringBuilder("[");
                    ListObjectInspector listObjectInspector = (ListObjectInspector) this.inputOI;
                    List<Object> elements = (List<Object>) listObjectInspector.getList(input);
                    TextConverter elementConverter = new TextConverter(
                            listObjectInspector.getListElementObjectInspector());
                    List<String> elementStringList = new ArrayList<>();
                    for (Object element : elements) {
                        Text elementConverted = elementConverter.convert(element);
                        elementStringList.add(elementConverted == null ? "null" :
                                elementConverted.toString());
                    }
                    builder.append(String.join(", ", elementStringList));
                    builder.append("]");
                    return new Text(builder.toString());
                } else if (this.inputOI instanceof PrimitiveObjectInspector) {
                    PrimitiveObjectInspector inputPrimitiveOI =
                            (PrimitiveObjectInspector) this.inputOI;
                    switch (inputPrimitiveOI.getPrimitiveCategory()) {
                        case VOID:
                            return null;
                        case BOOLEAN:
                            this.t.set(
                                    ((BooleanObjectInspector) this.inputOI).get(input)
                                            ? trueBytes
                                            : falseBytes);
                            return this.t;
                        case BYTE:
                            this.out.reset();
                            LazyInteger.writeUTF8NoException(
                                    this.out, ((ByteObjectInspector) this.inputOI).get(input));
                            this.t.set(this.out.getData(), 0, this.out.getLength());
                            return this.t;
                        case SHORT:
                            this.out.reset();
                            LazyInteger.writeUTF8NoException(
                                    this.out, ((ShortObjectInspector) this.inputOI).get(input));
                            this.t.set(this.out.getData(), 0, this.out.getLength());
                            return this.t;
                        case INT:
                            this.out.reset();
                            LazyInteger.writeUTF8NoException(
                                    this.out, ((IntObjectInspector) this.inputOI).get(input));
                            this.t.set(this.out.getData(), 0, this.out.getLength());
                            return this.t;
                        case LONG:
                            this.out.reset();
                            LazyLong.writeUTF8NoException(
                                    this.out, ((LongObjectInspector) this.inputOI).get(input));
                            this.t.set(this.out.getData(), 0, this.out.getLength());
                            return this.t;
                        case FLOAT:
                            this.t.set(
                                    String.valueOf(
                                            ((FloatObjectInspector) this.inputOI).get(input)));
                            return this.t;
                        case DOUBLE:
                            this.t.set(
                                    String.valueOf(
                                            ((DoubleObjectInspector) this.inputOI).get(input)));
                            return this.t;
                        case STRING:
                            if (inputPrimitiveOI.preferWritable()) {
                                this.t.set(
                                        ((StringObjectInspector) this.inputOI)
                                                .getPrimitiveWritableObject(input));
                            } else {
                                this.t.set(
                                        ((StringObjectInspector) this.inputOI)
                                                .getPrimitiveJavaObject(input));
                            }

                            return this.t;
                        case CHAR:
                            if (inputPrimitiveOI.preferWritable()) {
                                this.t.set(
                                        ((HiveCharObjectInspector) this.inputOI)
                                                .getPrimitiveWritableObject(input)
                                                .getStrippedValue());
                            } else {
                                this.t.set(
                                        ((HiveCharObjectInspector) this.inputOI)
                                                .getPrimitiveJavaObject(input)
                                                .getStrippedValue());
                            }

                            return this.t;
                        case VARCHAR:
                            if (inputPrimitiveOI.preferWritable()) {
                                this.t.set(
                                        ((HiveVarcharObjectInspector) this.inputOI)
                                                .getPrimitiveWritableObject(input)
                                                .toString());
                            } else {
                                this.t.set(
                                        ((HiveVarcharObjectInspector) this.inputOI)
                                                .getPrimitiveJavaObject(input)
                                                .toString());
                            }

                            return this.t;
                        case DATE:
                            this.t.set(
                                    ((DateObjectInspector) this.inputOI)
                                            .getPrimitiveWritableObject(input)
                                            .toString());
                            return this.t;
                        case TIMESTAMP:
                            this.t.set(
                                    ((TimestampObjectInspector) this.inputOI)
                                            .getPrimitiveWritableObject(input)
                                            .toString());
                            return this.t;
                        case INTERVAL_YEAR_MONTH:
                            this.t.set(
                                    ((HiveIntervalYearMonthObjectInspector) this.inputOI)
                                            .getPrimitiveWritableObject(input)
                                            .toString());
                            return this.t;
                        case INTERVAL_DAY_TIME:
                            this.t.set(
                                    ((HiveIntervalDayTimeObjectInspector) this.inputOI)
                                            .getPrimitiveWritableObject(input)
                                            .toString());
                            return this.t;
                        case BINARY:
                            BinaryObjectInspector binaryOI = (BinaryObjectInspector) this.inputOI;
                            if (binaryOI.preferWritable()) {
                                BytesWritable bytes = binaryOI.getPrimitiveWritableObject(input);
                                this.t.set(bytes.getBytes(), 0, bytes.getLength());
                            } else {
                                this.t.set(binaryOI.getPrimitiveJavaObject(input));
                            }

                            return this.t;
                        case DECIMAL:
                            this.t.set(
                                    ((HiveDecimalObjectInspector) this.inputOI)
                                            .getPrimitiveWritableObject(input)
                                            .toString());
                            return this.t;
                        default:
                            throw new RuntimeException(
                                    "Hive 2 Internal error: type = " + this.inputOI.getTypeName());
                    }

                } else {
                    throw new RuntimeException(
                            "Hive 2 Internal error: type = " + this.inputOI.getTypeName());
                }
            }
        }
    }
}

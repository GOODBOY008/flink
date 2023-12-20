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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichCrossFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.Either;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Row;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("serial")
public class TypeExtractorTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testBasicType() {
        // use getGroupReduceReturnTypes()
        RichGroupReduceFunction<?, ?> function =
                new RichGroupReduceFunction<Boolean, Boolean>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void reduce(Iterable<Boolean> values, Collector<Boolean> out) {
                        // nothing to do
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getGroupReduceReturnTypes(function, (TypeInformation) Types.BOOLEAN);

        Assertions.assertTrue(ti.isBasicType());
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
        Assertions.assertSame(Boolean.class, ti.getTypeClass());

        // use getForClass()
        Assertions.assertTrue(TypeExtractor.getForClass(Boolean.class).isBasicType());
        Assertions.assertEquals(ti, TypeExtractor.getForClass(Boolean.class));

        // use getForObject()
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, TypeExtractor.getForObject(true));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testTupleWithBasicTypes() {
        // use getMapReturnTypes()
        RichMapFunction<?, ?> function =
                new RichMapFunction<
                        Tuple9<
                                Integer,
                                Long,
                                Double,
                                Float,
                                Boolean,
                                String,
                                Character,
                                Short,
                                Byte>,
                        Tuple9<
                                Integer,
                                Long,
                                Double,
                                Float,
                                Boolean,
                                String,
                                Character,
                                Short,
                                Byte>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple9<
                                    Integer,
                                    Long,
                                    Double,
                                    Float,
                                    Boolean,
                                    String,
                                    Character,
                                    Short,
                                    Byte>
                            map(
                                    Tuple9<
                                                    Integer,
                                                    Long,
                                                    Double,
                                                    Float,
                                                    Boolean,
                                                    String,
                                                    Character,
                                                    Short,
                                                    Byte>
                                            value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(
                                        new TypeHint<
                                                Tuple9<
                                                        Integer,
                                                        Long,
                                                        Double,
                                                        Float,
                                                        Boolean,
                                                        String,
                                                        Character,
                                                        Short,
                                                        Byte>>() {}));

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(9, ti.getArity());
      Assertions.assertInstanceOf(TupleTypeInfo.class, ti);
        List<FlatFieldDescriptor> ffd = new ArrayList<>();
        ((TupleTypeInfo) ti).getFlatFields("f3", 0, ffd);
        Assertions.assertEquals(1, ffd.size());
        Assertions.assertEquals(3, ffd.get(0).getPosition());

        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertSame(Tuple9.class, tti.getTypeClass());

        for (int i = 0; i < 9; i++) {
          Assertions.assertInstanceOf(BasicTypeInfo.class, tti.getTypeAt(i));
        }

        Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1));
        Assertions.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tti.getTypeAt(2));
        Assertions.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti.getTypeAt(3));
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti.getTypeAt(4));
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(5));
        Assertions.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, tti.getTypeAt(6));
        Assertions.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, tti.getTypeAt(7));
        Assertions.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, tti.getTypeAt(8));

        // use getForObject()
        Tuple9<Integer, Long, Double, Float, Boolean, String, Character, Short, Byte> t =
                new Tuple9<>(1, 1L, 1.0, 1.0F, false, "Hello World", 'w', (short) 1, (byte) 1);

      Assertions.assertInstanceOf(TupleTypeInfo.class, TypeExtractor.getForObject(t));
        TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) TypeExtractor.getForObject(t);

        Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti2.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti2.getTypeAt(1));
        Assertions.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tti2.getTypeAt(2));
        Assertions.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti2.getTypeAt(3));
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti2.getTypeAt(4));
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti2.getTypeAt(5));
        Assertions.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, tti2.getTypeAt(6));
        Assertions.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, tti2.getTypeAt(7));
        Assertions.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, tti2.getTypeAt(8));

        // test that getForClass does not work
        try {
            TypeExtractor.getForClass(Tuple9.class);
            Assertions.fail("Exception expected here");
        } catch (InvalidTypesException e) {
            // that is correct
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testTupleWithTuples() {
        // use getFlatMapReturnTypes()
        RichFlatMapFunction<?, ?> function =
                new RichFlatMapFunction<
                        Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>,
                        Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void flatMap(
                            Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>> value,
                            Collector<Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>>
                                    out) {
                        // nothing to do
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getFlatMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(
                                        new TypeHint<
                                                Tuple3<
                                                        Tuple1<String>,
                                                        Tuple1<Integer>,
                                                        Tuple2<Long, Long>>>() {}));
        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(3, ti.getArity());
      Assertions.assertInstanceOf(TupleTypeInfo.class, ti);
        List<FlatFieldDescriptor> ffd = new ArrayList<>();

        ((TupleTypeInfo) ti).getFlatFields("f0.f0", 0, ffd);
        Assertions.assertEquals(0, ffd.get(0).getPosition());
        ffd.clear();

        ((TupleTypeInfo) ti).getFlatFields("f0.f0", 0, ffd);
      Assertions.assertInstanceOf(BasicTypeInfo.class, ffd.get(0).getType());
        Assertions.assertSame(ffd.get(0).getType().getTypeClass(), String.class);
        ffd.clear();

        ((TupleTypeInfo) ti).getFlatFields("f1.f0", 0, ffd);
        Assertions.assertEquals(1, ffd.get(0).getPosition());
        ffd.clear();

        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertSame(Tuple3.class, tti.getTypeClass());

        Assertions.assertTrue(tti.getTypeAt(0).isTupleType());
        Assertions.assertTrue(tti.getTypeAt(1).isTupleType());
        Assertions.assertTrue(tti.getTypeAt(2).isTupleType());

        Assertions.assertSame(Tuple1.class, tti.getTypeAt(0).getTypeClass());
        Assertions.assertSame(Tuple1.class, tti.getTypeAt(1).getTypeClass());
        Assertions.assertSame(Tuple2.class, tti.getTypeAt(2).getTypeClass());

        Assertions.assertEquals(1, tti.getTypeAt(0).getArity());
        Assertions.assertEquals(1, tti.getTypeAt(1).getArity());
        Assertions.assertEquals(2, tti.getTypeAt(2).getArity());

        Assertions.assertEquals(
                BasicTypeInfo.STRING_TYPE_INFO, ((TupleTypeInfo<?>) tti.getTypeAt(0)).getTypeAt(0));
        Assertions.assertEquals(
                BasicTypeInfo.INT_TYPE_INFO, ((TupleTypeInfo<?>) tti.getTypeAt(1)).getTypeAt(0));
        Assertions.assertEquals(
                BasicTypeInfo.LONG_TYPE_INFO, ((TupleTypeInfo<?>) tti.getTypeAt(2)).getTypeAt(0));
        Assertions.assertEquals(
                BasicTypeInfo.LONG_TYPE_INFO, ((TupleTypeInfo<?>) tti.getTypeAt(2)).getTypeAt(1));

        // use getForObject()
        Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>> t =
                new Tuple3<>(new Tuple1<>("hello"), new Tuple1<>(1), new Tuple2<>(2L, 3L));
      Assertions.assertInstanceOf(TupleTypeInfo.class, TypeExtractor.getForObject(t));
        TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) TypeExtractor.getForObject(t);

        Assertions.assertEquals(1, tti2.getTypeAt(0).getArity());
        Assertions.assertEquals(1, tti2.getTypeAt(1).getArity());
        Assertions.assertEquals(2, tti2.getTypeAt(2).getArity());

        Assertions.assertEquals(
                BasicTypeInfo.STRING_TYPE_INFO,
                ((TupleTypeInfo<?>) tti2.getTypeAt(0)).getTypeAt(0));
        Assertions.assertEquals(
                BasicTypeInfo.INT_TYPE_INFO, ((TupleTypeInfo<?>) tti2.getTypeAt(1)).getTypeAt(0));
        Assertions.assertEquals(
                BasicTypeInfo.LONG_TYPE_INFO, ((TupleTypeInfo<?>) tti2.getTypeAt(2)).getTypeAt(0));
        Assertions.assertEquals(
                BasicTypeInfo.LONG_TYPE_INFO, ((TupleTypeInfo<?>) tti2.getTypeAt(2)).getTypeAt(1));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testTuple0() {
        // use getFlatMapReturnTypes()
        RichFlatMapFunction<?, ?> function =
                new RichFlatMapFunction<Tuple0, Tuple0>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void flatMap(Tuple0 value, Collector<Tuple0> out) {
                        // nothing to do
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getFlatMapReturnTypes(
                        function, (TypeInformation) TypeInformation.of(new TypeHint<Tuple0>() {}));

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(0, ti.getArity());
      Assertions.assertInstanceOf(TupleTypeInfo.class, ti);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testSubclassOfTuple() {
        // use getJoinReturnTypes()
        RichFlatJoinFunction<?, ?, ?> function =
                new RichFlatJoinFunction<CustomTuple, String, CustomTuple>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void join(CustomTuple first, String second, Collector<CustomTuple> out) {
                        out.collect(null);
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getFlatJoinReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}),
                        (TypeInformation) Types.STRING);

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());
        Assertions.assertEquals(
                BasicTypeInfo.STRING_TYPE_INFO, ((TupleTypeInfo<?>) ti).getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ((TupleTypeInfo<?>) ti).getTypeAt(1));
        Assertions.assertSame(CustomTuple.class, ((TupleTypeInfo<?>) ti).getTypeClass());

        // use getForObject()
        CustomTuple t = new CustomTuple("hello", 1);
        TypeInformation<?> ti2 = TypeExtractor.getForObject(t);

        Assertions.assertTrue(ti2.isTupleType());
        Assertions.assertEquals(2, ti2.getArity());
        Assertions.assertEquals(
                BasicTypeInfo.STRING_TYPE_INFO, ((TupleTypeInfo<?>) ti2).getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ((TupleTypeInfo<?>) ti2).getTypeAt(1));
        Assertions.assertSame(CustomTuple.class, ((TupleTypeInfo<?>) ti2).getTypeClass());
    }

    public static class CustomTuple extends Tuple2<String, Integer> {
        private static final long serialVersionUID = 1L;

        public CustomTuple(String myField1, Integer myField2) {
            this.setFields(myField1, myField2);
        }

        public String getMyField1() {
            return this.f0;
        }

        public int getMyField2() {
            return this.f1;
        }
    }

    public static class PojoWithNonPublicDefaultCtor {
        public int foo, bar;

        PojoWithNonPublicDefaultCtor() {}
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testPojo() {
        // use getCrossReturnTypes()
        RichCrossFunction<?, ?, ?> function =
                new RichCrossFunction<CustomType, Integer, CustomType>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public CustomType cross(CustomType first, Integer second) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getCrossReturnTypes(
                        function,
                        (TypeInformation) TypeInformation.of(new TypeHint<CustomType>() {}),
                        (TypeInformation) Types.INT);

        Assertions.assertFalse(ti.isBasicType());
        Assertions.assertFalse(ti.isTupleType());
      Assertions.assertInstanceOf(PojoTypeInfo.class, ti);
        Assertions.assertSame(ti.getTypeClass(), CustomType.class);

        // use getForClass()
      Assertions.assertInstanceOf(PojoTypeInfo.class, TypeExtractor.getForClass(CustomType.class));
        Assertions.assertSame(
                TypeExtractor.getForClass(CustomType.class).getTypeClass(), ti.getTypeClass());

        // use getForObject()
        CustomType t = new CustomType("World", 1);
        TypeInformation<?> ti2 = TypeExtractor.getForObject(t);

        Assertions.assertFalse(ti2.isBasicType());
        Assertions.assertFalse(ti2.isTupleType());
      Assertions.assertInstanceOf(PojoTypeInfo.class, ti2);
        Assertions.assertSame(ti2.getTypeClass(), CustomType.class);

        Assertions.assertFalse(
                TypeExtractor.getForClass(PojoWithNonPublicDefaultCtor.class)
                        instanceof PojoTypeInfo);
    }

    @Test
    void testMethodChainingPojo() {
        CustomChainingPojoType t = new CustomChainingPojoType();
        t.setMyField1("World").setMyField2(1);
        TypeInformation<?> ti = TypeExtractor.getForObject(t);

        Assertions.assertFalse(ti.isBasicType());
        Assertions.assertFalse(ti.isTupleType());
      Assertions.assertInstanceOf(PojoTypeInfo.class, ti);
        Assertions.assertSame(ti.getTypeClass(), CustomChainingPojoType.class);
    }

    @Test
    void testRow() {
        Row row = new Row(2);
        row.setField(0, "string");
        row.setField(1, 15);
        TypeInformation<Row> rowInfo = TypeExtractor.getForObject(row);
        Assertions.assertSame(rowInfo.getClass(), RowTypeInfo.class);
        Assertions.assertEquals(2, rowInfo.getArity());
        Assertions.assertEquals(
                new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
                rowInfo);

        Row nullRow = new Row(2);
        TypeInformation<Row> genericRowInfo = TypeExtractor.getForObject(nullRow);
        Assertions.assertEquals(genericRowInfo, new GenericTypeInfo<>(Row.class));
    }

    public static class CustomType {
        public String myField1;
        public int myField2;

        public CustomType() {}

        public CustomType(String myField1, int myField2) {
            this.myField1 = myField1;
            this.myField2 = myField2;
        }
    }

    public static class CustomChainingPojoType {
        private String myField1;
        private int myField2;

        public CustomChainingPojoType() {}

        public CustomChainingPojoType setMyField1(String myField1) {
            this.myField1 = myField1;
            return this;
        }

        public CustomChainingPojoType setMyField2(int myField2) {
            this.myField2 = myField2;
            return this;
        }

        public String getMyField1() {
            return myField1;
        }

        public int getMyField2() {
            return myField2;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testTupleWithPojo() {
        // use getMapReturnTypes()
        RichMapFunction<?, ?> function =
                new RichMapFunction<Tuple2<Long, CustomType>, Tuple2<Long, CustomType>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, CustomType> map(Tuple2<Long, CustomType> value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(new TypeHint<Tuple2<Long, CustomType>>() {}));

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());

        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertSame(Tuple2.class, tti.getTypeClass());
        List<FlatFieldDescriptor> ffd = new ArrayList<>();

        tti.getFlatFields("f0", 0, ffd);
        Assertions.assertEquals(1, ffd.size());
        Assertions.assertEquals(0, ffd.get(0).getPosition()); // Long
        Assertions.assertSame(ffd.get(0).getType().getTypeClass(), Long.class);
        ffd.clear();

        tti.getFlatFields("f1.myField1", 0, ffd);
        Assertions.assertEquals(1, ffd.get(0).getPosition());
        Assertions.assertSame(ffd.get(0).getType().getTypeClass(), String.class);
        ffd.clear();

        tti.getFlatFields("f1.myField2", 0, ffd);
        Assertions.assertEquals(2, ffd.get(0).getPosition());
        Assertions.assertSame(ffd.get(0).getType().getTypeClass(), Integer.class);

        Assertions.assertSame(Long.class, tti.getTypeAt(0).getTypeClass());
      Assertions.assertInstanceOf(PojoTypeInfo.class, tti.getTypeAt(1));
        Assertions.assertSame(CustomType.class, tti.getTypeAt(1).getTypeClass());

        // use getForObject()
        Tuple2<?, ?> t = new Tuple2<>(1L, new CustomType("Hello", 1));
        TypeInformation<?> ti2 = TypeExtractor.getForObject(t);

        Assertions.assertTrue(ti2.isTupleType());
        Assertions.assertEquals(2, ti2.getArity());
        TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) ti2;

        Assertions.assertSame(Tuple2.class, tti2.getTypeClass());
        Assertions.assertSame(Long.class, tti2.getTypeAt(0).getTypeClass());
      Assertions.assertInstanceOf(PojoTypeInfo.class, tti2.getTypeAt(1));
        Assertions.assertSame(CustomType.class, tti2.getTypeAt(1).getTypeClass());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testValue() {
        // use getKeyExtractorType()
        KeySelector<?, ?> function =
                new KeySelector<StringValue, StringValue>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public StringValue getKey(StringValue value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getKeySelectorTypes(
                        function,
                        (TypeInformation) TypeInformation.of(new TypeHint<StringValue>() {}));

        Assertions.assertFalse(ti.isBasicType());
        Assertions.assertFalse(ti.isTupleType());
      Assertions.assertInstanceOf(ValueTypeInfo.class, ti);
        Assertions.assertSame(ti.getTypeClass(), StringValue.class);

        // use getForClass()
      Assertions.assertInstanceOf(ValueTypeInfo.class, TypeExtractor.getForClass(StringValue.class));
        Assertions.assertSame(
                TypeExtractor.getForClass(StringValue.class).getTypeClass(), ti.getTypeClass());

        // use getForObject()
        StringValue v = new StringValue("Hello");
      Assertions.assertInstanceOf(ValueTypeInfo.class, TypeExtractor.getForObject(v));
        Assertions.assertSame(TypeExtractor.getForObject(v).getTypeClass(), ti.getTypeClass());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testTupleOfValues() {
        // use getMapReturnTypes()
        RichMapFunction<?, ?> function =
                new RichMapFunction<
                        Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<StringValue, IntValue> map(Tuple2<StringValue, IntValue> value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(
                                        new TypeHint<Tuple2<StringValue, IntValue>>() {}));

        Assertions.assertFalse(ti.isBasicType());
        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertSame(
                StringValue.class, ((TupleTypeInfo<?>) ti).getTypeAt(0).getTypeClass());
        Assertions.assertSame(IntValue.class, ((TupleTypeInfo<?>) ti).getTypeAt(1).getTypeClass());

        // use getForObject()
        Tuple2<StringValue, IntValue> t = new Tuple2<>(new StringValue("x"), new IntValue(1));
        TypeInformation<?> ti2 = TypeExtractor.getForObject(t);

        Assertions.assertFalse(ti2.isBasicType());
        Assertions.assertTrue(ti2.isTupleType());
        Assertions.assertSame(
                ((TupleTypeInfo<?>) ti2).getTypeAt(0).getTypeClass(), StringValue.class);
        Assertions.assertSame(((TupleTypeInfo<?>) ti2).getTypeAt(1).getTypeClass(), IntValue.class);
    }

    public static class LongKeyValue<V> extends Tuple2<Long, V> {
        private static final long serialVersionUID = 1L;

        public LongKeyValue(Long field1, V field2) {
            this.f0 = field1;
            this.f1 = field2;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testGenericsNotInSuperclass() {
        // use getMapReturnTypes()
        RichMapFunction<?, ?> function =
                new RichMapFunction<LongKeyValue<String>, LongKeyValue<String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public LongKeyValue<String> map(LongKeyValue<String> value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {}));

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());

        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertSame(LongKeyValue.class, tti.getTypeClass());

        Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
    }

    public static class ChainedOne<X, Y> extends Tuple3<X, Long, Y> {
        private static final long serialVersionUID = 1L;

        public ChainedOne(X field0, Long field1, Y field2) {
            this.f0 = field0;
            this.f1 = field1;
            this.f2 = field2;
        }
    }

    public static class ChainedTwo<V> extends ChainedOne<String, V> {
        private static final long serialVersionUID = 1L;

        public ChainedTwo(String field0, Long field1, V field2) {
            super(field0, field1, field2);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testChainedGenericsNotInSuperclass() {
        // use TypeExtractor
        RichMapFunction<?, ?> function =
                new RichMapFunction<ChainedTwo<Integer>, ChainedTwo<Integer>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public ChainedTwo<Integer> map(ChainedTwo<Integer> value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(
                                        new TypeHint<Tuple3<String, Long, Integer>>() {}));

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(3, ti.getArity());

        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertSame(ChainedTwo.class, tti.getTypeClass());

        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1));
        Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti.getTypeAt(2));
    }

    public static class ChainedThree extends ChainedTwo<String> {
        private static final long serialVersionUID = 1L;

        public ChainedThree(String field0, Long field1, String field2) {
            super(field0, field1, field2);
        }
    }

    public static class ChainedFour extends ChainedThree {
        private static final long serialVersionUID = 1L;

        public ChainedFour(String field0, Long field1, String field2) {
            super(field0, field1, field2);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testGenericsInDirectSuperclass() {
        // use TypeExtractor
        RichMapFunction<?, ?> function =
                new RichMapFunction<ChainedThree, ChainedThree>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public ChainedThree map(ChainedThree value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(
                                        new TypeHint<Tuple3<String, Long, String>>() {}));

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(3, ti.getArity());

        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertSame(ChainedThree.class, tti.getTypeClass());

        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1));
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(2));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testGenericsNotInSuperclassWithNonGenericClassAtEnd() {
        // use TypeExtractor
        RichMapFunction<?, ?> function =
                new RichMapFunction<ChainedFour, ChainedFour>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public ChainedFour map(ChainedFour value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(
                                        new TypeHint<Tuple3<String, Long, String>>() {}));

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(3, ti.getArity());

        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertSame(ChainedFour.class, tti.getTypeClass());

        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1));
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(2));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testMissingTupleGenerics() {
        RichMapFunction<?, ?> function =
                new RichMapFunction<String, Tuple2>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2 map(String value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function, (TypeInformation) Types.STRING, "name", true);
      Assertions.assertInstanceOf(MissingTypeInfo.class, ti);

        try {
            TypeExtractor.getMapReturnTypes(function, (TypeInformation) Types.STRING);
            Assertions.fail("Expected an exception");
        } catch (InvalidTypesException e) {
            // expected
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testTupleSupertype() {
        RichMapFunction<?, ?> function =
                new RichMapFunction<String, Tuple>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple map(String value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function, (TypeInformation) Types.STRING, "name", true);
      Assertions.assertInstanceOf(MissingTypeInfo.class, ti);

        try {
            TypeExtractor.getMapReturnTypes(function, (TypeInformation) Types.STRING);
            Assertions.fail("Expected an exception");
        } catch (InvalidTypesException e) {
            // expected
        }
    }

    public static class SameTypeVariable<X> extends Tuple2<X, X> {
        private static final long serialVersionUID = 1L;

        public SameTypeVariable(X field0, X field1) {
            super(field0, field1);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testSameGenericVariable() {
        RichMapFunction<?, ?> function =
                new RichMapFunction<SameTypeVariable<String>, SameTypeVariable<String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public SameTypeVariable<String> map(SameTypeVariable<String> value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());

        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertSame(SameTypeVariable.class, tti.getTypeClass());

        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
    }

    public static class Nested<V, T> extends Tuple2<V, Tuple2<T, T>> {
        private static final long serialVersionUID = 1L;

        public Nested(V field0, Tuple2<T, T> field1) {
            super(field0, field1);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testNestedTupleGenerics() {
        RichMapFunction<?, ?> function =
                new RichMapFunction<Nested<String, Integer>, Nested<String, Integer>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Nested<String, Integer> map(Nested<String, Integer> value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(
                                        new TypeHint<
                                                Tuple2<String, Tuple2<Integer, Integer>>>() {}));

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());

        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertSame(Nested.class, tti.getTypeClass());

        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertTrue(tti.getTypeAt(1).isTupleType());
        Assertions.assertEquals(2, tti.getTypeAt(1).getArity());

        // Nested
        TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) tti.getTypeAt(1);
        Assertions.assertSame(Tuple2.class, tti2.getTypeClass());
        Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti2.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti2.getTypeAt(1));
    }

    public static class Nested2<T> extends Nested<T, Nested<Integer, T>> {
        private static final long serialVersionUID = 1L;

        public Nested2(T field0, Tuple2<Nested<Integer, T>, Nested<Integer, T>> field1) {
            super(field0, field1);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testNestedTupleGenerics2() {
        RichMapFunction<?, ?> function =
                new RichMapFunction<Nested2<Boolean>, Nested2<Boolean>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Nested2<Boolean> map(Nested2<Boolean> value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(
                                        new TypeHint<
                                                Tuple2<
                                                        Boolean,
                                                        Tuple2<
                                                                Tuple2<
                                                                        Integer,
                                                                        Tuple2<Boolean, Boolean>>,
                                                                Tuple2<
                                                                        Integer,
                                                                        Tuple2<
                                                                                Boolean,
                                                                                Boolean>>>>>() {}));

        // Should be
        // Tuple2<Boolean, Tuple2<Tuple2<Integer, Tuple2<Boolean, Boolean>>, Tuple2<Integer,
        // Tuple2<Boolean, Boolean>>>>

        // 1st nested level
        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertTrue(tti.getTypeAt(1).isTupleType());

        // 2nd nested level
        TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) tti.getTypeAt(1);
        Assertions.assertTrue(tti2.getTypeAt(0).isTupleType());
        Assertions.assertTrue(tti2.getTypeAt(1).isTupleType());

        // 3rd nested level
        TupleTypeInfo<?> tti3 = (TupleTypeInfo<?>) tti2.getTypeAt(0);
        Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti3.getTypeAt(0));
        Assertions.assertTrue(tti3.getTypeAt(1).isTupleType());

        // 4th nested level
        TupleTypeInfo<?> tti4 = (TupleTypeInfo<?>) tti3.getTypeAt(1);
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti4.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti4.getTypeAt(1));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testFunctionWithMissingGenerics() {
        RichMapFunction function =
                new RichMapFunction() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public String map(Object value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(function, Types.STRING, "name", true);
      Assertions.assertInstanceOf(MissingTypeInfo.class, ti);

        try {
            TypeExtractor.getMapReturnTypes(function, Types.STRING);
            Assertions.fail("Expected an exception");
        } catch (InvalidTypesException e) {
            // expected
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testFunctionDependingOnInputAsSuperclass() {
        IdentityMapper<Boolean> function =
                new IdentityMapper<Boolean>() {
                    private static final long serialVersionUID = 1L;
                };

        TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, Types.BOOLEAN);

        Assertions.assertTrue(ti.isBasicType());
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
    }

    public class IdentityMapper<T> extends RichMapFunction<T, T> {
        private static final long serialVersionUID = 1L;

        @Override
        public T map(T value) {
            return null;
        }
    }

    @Test
    void testFunctionDependingOnInputFromInput() {
        IdentityMapper<Boolean> function = new IdentityMapper<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.BOOLEAN_TYPE_INFO);

        Assertions.assertTrue(ti.isBasicType());
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
    }

    @Test
    void testFunctionDependingOnInputWithMissingInput() {
        IdentityMapper<Boolean> function = new IdentityMapper<>();

        try {
            TypeExtractor.getMapReturnTypes(function, null);
            Assertions.fail("exception expected");
        } catch (InvalidTypesException e) {
            // right
        }
    }

    public class IdentityMapper2<T> extends RichMapFunction<Tuple2<T, String>, T> {
        private static final long serialVersionUID = 1L;

        @Override
        public T map(Tuple2<T, String> value) {
            return null;
        }
    }

    @Test
    void testFunctionDependingOnInputWithTupleInput() {
        IdentityMapper2<Boolean> function = new IdentityMapper2<>();

        TypeInformation<Tuple2<Boolean, String>> inputType =
                new TupleTypeInfo<>(
                        BasicTypeInfo.BOOLEAN_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, inputType);

        Assertions.assertTrue(ti.isBasicType());
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testFunctionDependingOnInputWithCustomTupleInput() {
        IdentityMapper<SameTypeVariable<String>> function = new IdentityMapper<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
    }

    public class IdentityMapper3<T, V> extends RichMapFunction<T, V> {
        private static final long serialVersionUID = 1L;

        @Override
        public V map(T value) {
            return null;
        }
    }

    @Test
    void testFunctionDependingOnUnknownInput() {
        IdentityMapper3<Boolean, String> function = new IdentityMapper3<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function, BasicTypeInfo.BOOLEAN_TYPE_INFO, "name", true);
      Assertions.assertInstanceOf(MissingTypeInfo.class, ti);

        try {
            TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.BOOLEAN_TYPE_INFO);
            Assertions.fail("Expected an exception");
        } catch (InvalidTypesException e) {
            // expected
        }
    }

    public class IdentityMapper4<D> extends IdentityMapper<D> {
        private static final long serialVersionUID = 1L;
    }

    @Test
    void testFunctionDependingOnInputWithFunctionHierarchy() {
        IdentityMapper4<String> function = new IdentityMapper4<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.STRING_TYPE_INFO);

        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ti);
    }

    public class IdentityMapper5<D> extends IdentityMapper<Tuple2<D, D>> {
        private static final long serialVersionUID = 1L;
    }

    @Test
    void testFunctionDependingOnInputWithFunctionHierarchy2() {
        IdentityMapper5<String> function = new IdentityMapper5<>();

        @SuppressWarnings({"rawtypes", "unchecked"})
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        new TupleTypeInfo(
                                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

        Assertions.assertTrue(ti.isTupleType());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
    }

    public class Mapper extends IdentityMapper<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String map(String value) {
            return null;
        }
    }

    public class Mapper2 extends Mapper {
        private static final long serialVersionUID = 1L;

        @Override
        public String map(String value) {
            return null;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testFunctionWithNoGenericSuperclass() {
        RichMapFunction<?, ?> function = new Mapper2();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(function, (TypeInformation) Types.STRING);

        Assertions.assertTrue(ti.isBasicType());
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ti);
    }

    public class OneAppender<T> extends RichMapFunction<T, Tuple2<T, Integer>> {
        private static final long serialVersionUID = 1L;

        public Tuple2<T, Integer> map(T value) {
            return new Tuple2<>(value, 1);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testFunctionDependingPartialOnInput() {
        RichMapFunction<?, ?> function =
                new OneAppender<DoubleValue>() {
                    private static final long serialVersionUID = 1L;
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation) TypeInformation.of(new TypeHint<DoubleValue>() {}));

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;

      Assertions.assertInstanceOf(ValueTypeInfo<?>.class, tti.getTypeAt(0));
        ValueTypeInfo<?> vti = (ValueTypeInfo<?>) tti.getTypeAt(0);
        Assertions.assertSame(DoubleValue.class, vti.getTypeClass());

        Assertions.assertTrue(tti.getTypeAt(1).isBasicType());
        Assertions.assertSame(Integer.class, tti.getTypeAt(1).getTypeClass());
    }

    @Test
    void testFunctionDependingPartialOnInput2() {
        RichMapFunction<DoubleValue, ?> function = new OneAppender<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(function, new ValueTypeInfo<>(DoubleValue.class));

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;

      Assertions.assertInstanceOf(ValueTypeInfo<?>.class, tti.getTypeAt(0));
        ValueTypeInfo<?> vti = (ValueTypeInfo<?>) tti.getTypeAt(0);
        Assertions.assertSame(DoubleValue.class, vti.getTypeClass());

        Assertions.assertTrue(tti.getTypeAt(1).isBasicType());
        Assertions.assertSame(Integer.class, tti.getTypeAt(1).getTypeClass());
    }

    public class FieldDuplicator<T> extends RichMapFunction<T, Tuple2<T, T>> {
        private static final long serialVersionUID = 1L;

        public Tuple2<T, T> map(T value) {
            return new Tuple2<>(value, value);
        }
    }

    @Test
    void testFunctionInputInOutputMultipleTimes() {
        RichMapFunction<Float, ?> function = new FieldDuplicator<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.FLOAT_TYPE_INFO);

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti.getTypeAt(1));
    }

    @Test
    void testFunctionInputInOutputMultipleTimes2() {
        RichMapFunction<Tuple2<Float, Float>, ?> function = new FieldDuplicator<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        new TupleTypeInfo<>(
                                BasicTypeInfo.FLOAT_TYPE_INFO, BasicTypeInfo.FLOAT_TYPE_INFO));

        // should be
        // Tuple2<Tuple2<Float, Float>, Tuple2<Float, Float>>

        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;

        // 2nd nested level
        Assertions.assertTrue(tti.getTypeAt(0).isTupleType());
        TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) tti.getTypeAt(0);
        Assertions.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti2.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti2.getTypeAt(1));
        Assertions.assertTrue(tti.getTypeAt(0).isTupleType());
        TupleTypeInfo<?> tti3 = (TupleTypeInfo<?>) tti.getTypeAt(1);
        Assertions.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti3.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti3.getTypeAt(1));
    }

    public interface Testable {}

    public abstract static class AbstractClassWithoutMember {}

    public abstract static class AbstractClassWithMember {
        public int x;
    }

    @Test
    void testAbstractAndInterfaceTypes() {

        // interface
        RichMapFunction<String, ?> function =
                new RichMapFunction<String, Testable>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Testable map(String value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.STRING_TYPE_INFO);
      Assertions.assertInstanceOf(GenericTypeInfo.class, ti);

        // abstract class with out class member
        RichMapFunction<String, ?> function2 =
                new RichMapFunction<String, AbstractClassWithoutMember>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public AbstractClassWithoutMember map(String value) {
                        return null;
                    }
                };

        ti = TypeExtractor.getMapReturnTypes(function2, BasicTypeInfo.STRING_TYPE_INFO);
      Assertions.assertInstanceOf(GenericTypeInfo.class, ti);

        // abstract class with class member
        RichMapFunction<String, ?> function3 =
                new RichMapFunction<String, AbstractClassWithMember>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public AbstractClassWithMember map(String value) {
                        return null;
                    }
                };

        ti = TypeExtractor.getMapReturnTypes(function3, BasicTypeInfo.STRING_TYPE_INFO);
      Assertions.assertInstanceOf(PojoTypeInfo.class, ti);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testValueSupertypeException() {
        RichMapFunction<?, ?> function =
                new RichMapFunction<StringValue, Value>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Value map(StringValue value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation) TypeInformation.of(new TypeHint<StringValue>() {}),
                        "name",
                        true);
      Assertions.assertInstanceOf(MissingTypeInfo.class, ti);

        try {
            TypeExtractor.getMapReturnTypes(
                    function, (TypeInformation) TypeInformation.of(new TypeHint<StringValue>() {}));
            Assertions.fail("Expected an exception");
        } catch (InvalidTypesException e) {
            // expected
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testBasicArray() {
        // use getCoGroupReturnTypes()
        RichCoGroupFunction<?, ?, ?> function =
                new RichCoGroupFunction<String[], String[], String[]>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void coGroup(
                            Iterable<String[]> first,
                            Iterable<String[]> second,
                            Collector<String[]> out) {
                        // nothing to do
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getCoGroupReturnTypes(
                        function,
                        (TypeInformation) TypeInformation.of(new TypeHint<String[]>() {}),
                        (TypeInformation) TypeInformation.of(new TypeHint<String[]>() {}));

        Assertions.assertFalse(ti.isBasicType());
        Assertions.assertFalse(ti.isTupleType());

        // Due to a Java 6 bug the classification can be slightly wrong
        Assertions.assertTrue(
                ti instanceof BasicArrayTypeInfo<?, ?> || ti instanceof ObjectArrayTypeInfo<?, ?>);

        if (ti instanceof BasicArrayTypeInfo<?, ?>) {
            Assertions.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, ti);
        } else {
            Assertions.assertEquals(
                    BasicTypeInfo.STRING_TYPE_INFO,
                    ((ObjectArrayTypeInfo<?, ?>) ti).getComponentInfo());
        }
    }

    @Test
    void testBasicArray2() {
        RichMapFunction<Boolean[], ?> function = new IdentityMapper<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function, BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO);

      Assertions.assertInstanceOf(BasicArrayTypeInfo<?, ?>.class, ti);
        BasicArrayTypeInfo<?, ?> bati = (BasicArrayTypeInfo<?, ?>) ti;
        Assertions.assertTrue(bati.getComponentInfo().isBasicType());
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, bati.getComponentInfo());
    }

    public static class CustomArrayObject {}

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testCustomArray() {
        RichMapFunction<?, ?> function =
                new RichMapFunction<CustomArrayObject[], CustomArrayObject[]>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public CustomArrayObject[] map(CustomArrayObject[] value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(new TypeHint<CustomArrayObject[]>() {}));

      Assertions.assertInstanceOf(ObjectArrayTypeInfo<?, ?>.class, ti);
        Assertions.assertSame(
                CustomArrayObject.class,
                ((ObjectArrayTypeInfo<?, ?>) ti).getComponentInfo().getTypeClass());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testTupleArray() {
        RichMapFunction<?, ?> function =
                new RichMapFunction<Tuple2<String, String>[], Tuple2<String, String>[]>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String>[] map(Tuple2<String, String>[] value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(new TypeHint<Tuple2<String, String>[]>() {}));

      Assertions.assertInstanceOf(ObjectArrayTypeInfo<?, ?>.class, ti);
        ObjectArrayTypeInfo<?, ?> oati = (ObjectArrayTypeInfo<?, ?>) ti;
        Assertions.assertTrue(oati.getComponentInfo().isTupleType());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) oati.getComponentInfo();
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
    }

    public class CustomArrayObject2<F> extends Tuple1<F> {
        private static final long serialVersionUID = 1L;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testCustomArrayWithTypeVariable() {
        RichMapFunction<CustomArrayObject2<Boolean>[], ?> function = new IdentityMapper<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation) TypeInformation.of(new TypeHint<Tuple1<Boolean>[]>() {}));

      Assertions.assertInstanceOf(ObjectArrayTypeInfo<?, ?>.class, ti);
        ObjectArrayTypeInfo<?, ?> oati = (ObjectArrayTypeInfo<?, ?>) ti;
        Assertions.assertTrue(oati.getComponentInfo().isTupleType());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) oati.getComponentInfo();
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti.getTypeAt(0));
    }

    public class GenericArrayClass<T> extends RichMapFunction<T[], T[]> {
        private static final long serialVersionUID = 1L;

        @Override
        public T[] map(T[] value) {
            return null;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testParameterizedArrays() {
        GenericArrayClass<Boolean> function =
                new GenericArrayClass<Boolean>() {
                    private static final long serialVersionUID = 1L;
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function, TypeInformation.of(new TypeHint<Boolean[]>() {}));
      Assertions.assertInstanceOf(ObjectArrayTypeInfo<?, ?>.class, ti);
        ObjectArrayTypeInfo<?, ?> oati = (ObjectArrayTypeInfo<?, ?>) ti;
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, oati.getComponentInfo());
    }

    public static class MyObject<T> {
        public T myField;
    }

    public static class InType extends MyObject<String> {}

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testParameterizedPojo() {
        RichMapFunction<?, ?> function =
                new RichMapFunction<InType, MyObject<String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public MyObject<String> map(InType value) {
                        return null;
                    }
                };
        TypeInformation<?> inType = TypeExtractor.createTypeInfo(InType.class);
        TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) inType);
      Assertions.assertInstanceOf(PojoTypeInfo.class, ti);
    }

    @Test
    void testFunctionDependingOnInputWithTupleInputWithTypeMismatch() {
        IdentityMapper2<Boolean> function = new IdentityMapper2<>();

        TypeInformation<Tuple2<Boolean, String>> inputType =
                new TupleTypeInfo<>(BasicTypeInfo.BOOLEAN_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

        // input is: Tuple2<Boolean, Integer>
        // allowed: Tuple2<?, String>

        try {
            TypeExtractor.getMapReturnTypes(function, inputType);
            Assertions.fail("exception expected");
        } catch (InvalidTypesException e) {
            // right
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testInputMismatchExceptions() {

        RichMapFunction<?, ?> function =
                new RichMapFunction<Tuple2<String, String>, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public String map(Tuple2<String, String> value) {
                        return null;
                    }
                };

        try {
            TypeExtractor.getMapReturnTypes(
                    function,
                    (TypeInformation)
                            TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {}));
            Assertions.fail("exception expected");
        } catch (InvalidTypesException e) {
            // right
        }

        try {
            TypeExtractor.getMapReturnTypes(
                    function,
                    (TypeInformation)
                            TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {}));
            Assertions.fail("exception expected");
        } catch (InvalidTypesException e) {
            // right
        }

        RichMapFunction<?, ?> function2 =
                new RichMapFunction<StringValue, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public String map(StringValue value) {
                        return null;
                    }
                };

        try {
            TypeExtractor.getMapReturnTypes(
                    function2, (TypeInformation) TypeInformation.of(new TypeHint<IntValue>() {}));
            Assertions.fail("exception expected");
        } catch (InvalidTypesException e) {
            // right
        }

        RichMapFunction<?, ?> function3 =
                new RichMapFunction<Tuple1<Integer>[], String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public String map(Tuple1<Integer>[] value) {
                        return null;
                    }
                };

        try {
            TypeExtractor.getMapReturnTypes(
                    function3, (TypeInformation) TypeInformation.of(new TypeHint<Integer[]>() {}));
            Assertions.fail("exception expected");
        } catch (InvalidTypesException e) {
            // right
        }
    }

    public static class DummyFlatMapFunction<A, B, C, D>
            extends RichFlatMapFunction<Tuple2<A, B>, Tuple2<C, D>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(Tuple2<A, B> value, Collector<Tuple2<C, D>> out) {}
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testTypeErasure() {
        TypeInformation<?> ti =
                TypeExtractor.getFlatMapReturnTypes(
                        new DummyFlatMapFunction<String, Integer, String, Boolean>(),
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}),
                        "name",
                        true);
      Assertions.assertInstanceOf(MissingTypeInfo.class, ti);

        try {
            TypeExtractor.getFlatMapReturnTypes(
                    new DummyFlatMapFunction<String, Integer, String, Boolean>(),
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

            Assertions.fail("Expected an exception");
        } catch (InvalidTypesException e) {
            // expected
        }
    }

    public static class MyQueryableMapper<A> extends RichMapFunction<String, A>
            implements ResultTypeQueryable<A> {
        private static final long serialVersionUID = 1L;

        @SuppressWarnings("unchecked")
        @Override
        public TypeInformation<A> getProducedType() {
            return (TypeInformation<A>) BasicTypeInfo.INT_TYPE_INFO;
        }

        @Override
        public A map(String value) {
            return null;
        }
    }

    @Test
    void testResultTypeQueryable() {
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        new MyQueryableMapper<Integer>(), BasicTypeInfo.STRING_TYPE_INFO);
        Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ti);
    }

    @Test
    void testTupleWithPrimitiveArray() {
        RichMapFunction<
                        Integer,
                        Tuple9<
                                int[],
                                double[],
                                long[],
                                byte[],
                                char[],
                                float[],
                                short[],
                                boolean[],
                                String[]>>
                function =
                        new RichMapFunction<
                                Integer,
                                Tuple9<
                                        int[],
                                        double[],
                                        long[],
                                        byte[],
                                        char[],
                                        float[],
                                        short[],
                                        boolean[],
                                        String[]>>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Tuple9<
                                            int[],
                                            double[],
                                            long[],
                                            byte[],
                                            char[],
                                            float[],
                                            short[],
                                            boolean[],
                                            String[]>
                                    map(Integer value) {
                                return null;
                            }
                        };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.INT_TYPE_INFO);
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertEquals(
                PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(
                PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(1));
        Assertions.assertEquals(
                PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(2));
        Assertions.assertEquals(
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(3));
        Assertions.assertEquals(
                PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(4));
        Assertions.assertEquals(
                PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(5));
        Assertions.assertEquals(
                PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(6));
        Assertions.assertEquals(
                PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(7));
        Assertions.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, tti.getTypeAt(8));
    }

    @Test
    void testFunction() {
        RichMapFunction<String, Boolean> mapInterface =
                new RichMapFunction<String, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void setRuntimeContext(RuntimeContext t) {}

                    @Override
                    public void open(OpenContext openContext) {}

                    @Override
                    public RuntimeContext getRuntimeContext() {
                        return null;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public Boolean map(String record) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(mapInterface, BasicTypeInfo.STRING_TYPE_INFO);
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
    }

    @Test
    void testInterface() {
        MapFunction<String, Boolean> mapInterface =
                new MapFunction<String, Boolean>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean map(String record) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(mapInterface, BasicTypeInfo.STRING_TYPE_INFO);
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
    }

    @Test
    void testCreateTypeInfoFromInstance() {
        ResultTypeQueryable instance =
                (ResultTypeQueryable<Long>) () -> BasicTypeInfo.LONG_TYPE_INFO;
        TypeInformation<?> ti = TypeExtractor.createTypeInfo(instance, null, null, 0);
        Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, ti);

        // method also needs to work for instances that do not implement ResultTypeQueryable
        MapFunction<Integer, Long> func = Integer::longValue;
        ti = TypeExtractor.createTypeInfo(func, MapFunction.class, func.getClass(), 0);
        Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ti);
    }

    @SuppressWarnings({"serial", "unchecked", "rawtypes"})
    @Test
    void testExtractKeySelector() {
        KeySelector<String, Integer> selector = value -> null;

        TypeInformation<?> ti =
                TypeExtractor.getKeySelectorTypes(selector, BasicTypeInfo.STRING_TYPE_INFO);
        Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ti);

        try {
            TypeExtractor.getKeySelectorTypes(
                    (KeySelector) selector, BasicTypeInfo.BOOLEAN_TYPE_INFO);
            Assertions.fail();
        } catch (InvalidTypesException e) {
            // good
        } catch (Exception e) {
            Assertions.fail("wrong exception type");
        }
    }

    public static class DuplicateValue<T> implements MapFunction<Tuple1<T>, Tuple2<T, T>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<T, T> map(Tuple1<T> vertex) {
            return new Tuple2<>(vertex.f0, vertex.f0);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testDuplicateValue() {
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) new DuplicateValue<String>(),
                        TypeInformation.of(new TypeHint<Tuple1<String>>() {}));
        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
    }

    public static class DuplicateValueNested<T>
            implements MapFunction<Tuple1<Tuple1<T>>, Tuple2<T, T>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<T, T> map(Tuple1<Tuple1<T>> vertex) {
            return new Tuple2<>(null, null);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testDuplicateValueNested() {
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) new DuplicateValueNested<String>(),
                        TypeInformation.of(new TypeHint<Tuple1<Tuple1<String>>>() {}));
        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(2, ti.getArity());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
    }

    public static class Edge<K, V> extends Tuple3<K, K, V> {
        private static final long serialVersionUID = 1L;
    }

    public static class EdgeMapper<K, V> implements MapFunction<Edge<K, V>, Edge<K, V>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Edge<K, V> map(Edge<K, V> value) {
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testInputInference1() {
        EdgeMapper<String, Double> em = new EdgeMapper<>();
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) em,
                        TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {}));
        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(3, ti.getArity());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
        Assertions.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tti.getTypeAt(2));
    }

    public static class EdgeMapper2<V> implements MapFunction<V, Edge<Long, V>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Edge<Long, V> map(V value) {
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testInputInference2() {
        EdgeMapper2<Boolean> em = new EdgeMapper2<>();
        TypeInformation<?> ti = TypeExtractor.getMapReturnTypes((MapFunction) em, Types.BOOLEAN);
        Assertions.assertTrue(ti.isTupleType());
        Assertions.assertEquals(3, ti.getArity());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1));
        Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti.getTypeAt(2));
    }

    public static class EdgeMapper3<K, V> implements MapFunction<Edge<K, V>, V> {
        private static final long serialVersionUID = 1L;

        @Override
        public V map(Edge<K, V> value) {
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testInputInference3() {
        EdgeMapper3<Boolean, String> em = new EdgeMapper3<>();
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) em,
                        TypeInformation.of(new TypeHint<Tuple3<Boolean, Boolean, String>>() {}));
        Assertions.assertTrue(ti.isBasicType());
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ti);
    }

    public static class EdgeMapper4<K, V> implements MapFunction<Edge<K, V>[], V> {
        private static final long serialVersionUID = 1L;

        @Override
        public V map(Edge<K, V>[] value) {
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testInputInference4() {
        EdgeMapper4<Boolean, String> em = new EdgeMapper4<>();
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) em,
                        TypeInformation.of(new TypeHint<Tuple3<Boolean, Boolean, String>[]>() {}));
        Assertions.assertTrue(ti.isBasicType());
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ti);
    }

    public static class CustomTuple2WithArray<K> extends Tuple2<K[], K> {

        public CustomTuple2WithArray() {
            // default constructor
        }
    }

    public class JoinWithCustomTuple2WithArray<T>
            extends RichJoinFunction<
                    CustomTuple2WithArray<T>, CustomTuple2WithArray<T>, CustomTuple2WithArray<T>> {

        @Override
        public CustomTuple2WithArray<T> join(
                CustomTuple2WithArray<T> first, CustomTuple2WithArray<T> second) {
            return null;
        }
    }

    @Test
    void testInputInferenceWithCustomTupleAndRichFunction() {
        JoinFunction<
                        CustomTuple2WithArray<Long>,
                        CustomTuple2WithArray<Long>,
                        CustomTuple2WithArray<Long>>
                function = new JoinWithCustomTuple2WithArray<>();

        TypeInformation<?> ti =
                TypeExtractor.getJoinReturnTypes(
                        function,
                        new TypeHint<CustomTuple2WithArray<Long>>() {}.getTypeInfo(),
                        new TypeHint<CustomTuple2WithArray<Long>>() {}.getTypeInfo());

        Assertions.assertTrue(ti.isTupleType());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1));

      Assertions.assertInstanceOf(ObjectArrayTypeInfo<?, ?>.class, tti.getTypeAt(0));
        ObjectArrayTypeInfo<?, ?> oati = (ObjectArrayTypeInfo<?, ?>) tti.getTypeAt(0);
        Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, oati.getComponentInfo());
    }

    public enum MyEnum {
        ONE,
        TWO,
        THREE
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testEnumType() {
        MapFunction<?, ?> mf =
                new MapFunction<MyEnum, MyEnum>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public MyEnum map(MyEnum value) {
                        return null;
                    }
                };

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes((MapFunction) mf, new EnumTypeInfo(MyEnum.class));
      Assertions.assertInstanceOf(EnumTypeInfo.class, ti);
        Assertions.assertSame(ti.getTypeClass(), MyEnum.class);
    }

    public static class MapperWithMultiDimGenericArray<T>
            implements MapFunction<T[][][], Tuple1<T>[][][]> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple1<T>[][][] map(T[][][] value) {
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testMultiDimensionalArray() {
        // tuple array
        MapFunction<?, ?> function =
                new MapFunction<Tuple2<Integer, Double>[][], Tuple2<Integer, Double>[][]>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Integer, Double>[][] map(Tuple2<Integer, Double>[][] value) {
                        return null;
                    }
                };
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) function,
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Double>[][]>() {}));
        Assertions.assertEquals(
                "ObjectArrayTypeInfo<ObjectArrayTypeInfo<Java Tuple2<Integer, Double>>>",
                ti.toString());

        // primitive array
        function =
                new MapFunction<int[][][], int[][][]>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public int[][][] map(int[][][] value) {
                        return null;
                    }
                };
        ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) function, TypeInformation.of(new TypeHint<int[][][]>() {}));
        Assertions.assertEquals("ObjectArrayTypeInfo<ObjectArrayTypeInfo<int[]>>", ti.toString());

        // basic array
        function =
                new MapFunction<Integer[][][], Integer[][][]>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer[][][] map(Integer[][][] value) {
                        return null;
                    }
                };
        ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) function,
                        TypeInformation.of(new TypeHint<Integer[][][]>() {}));
        Assertions.assertEquals(
                "ObjectArrayTypeInfo<ObjectArrayTypeInfo<BasicArrayTypeInfo<Integer>>>",
                ti.toString());

        // pojo array
        function =
                new MapFunction<CustomType[][][], CustomType[][][]>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public CustomType[][][] map(CustomType[][][] value) {
                        return null;
                    }
                };
        ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) function,
                        TypeInformation.of(new TypeHint<CustomType[][][]>() {}));

        Assertions.assertEquals(
                "ObjectArrayTypeInfo<ObjectArrayTypeInfo<ObjectArrayTypeInfo<"
                        + "PojoType<org.apache.flink.api.java.typeutils.TypeExtractorTest$CustomType, fields = [myField1: String, myField2: Integer]>"
                        + ">>>",
                ti.toString());

        // generic array
        ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) new MapperWithMultiDimGenericArray<String>(),
                        TypeInformation.of(new TypeHint<String[][][]>() {}));
        Assertions.assertEquals(
                "ObjectArrayTypeInfo<ObjectArrayTypeInfo<ObjectArrayTypeInfo<Java Tuple1<String>>>>",
                ti.toString());
    }

    @SuppressWarnings("rawtypes")
    public static class MapWithResultTypeQueryable implements MapFunction, ResultTypeQueryable {
        private static final long serialVersionUID = 1L;

        @Override
        public TypeInformation getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }

        @Override
        public Object map(Object value) {
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testInputMismatchWithRawFuntion() {
        MapFunction<?, ?> function = new MapWithResultTypeQueryable();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) function, BasicTypeInfo.INT_TYPE_INFO);
        Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ti);
    }

    public static class Either1<T> extends Either<String, T> {
        @Override
        public String left() throws IllegalStateException {
            return null;
        }

        @Override
        public T right() throws IllegalStateException {
            return null;
        }
    }

    public static class Either2 extends Either1<Tuple1<Integer>> {
        // nothing to do here
    }

    public static class EitherMapper<T> implements MapFunction<T, Either1<T>> {
        @Override
        public Either1<T> map(T value) {
            return null;
        }
    }

    public static class EitherMapper2 implements MapFunction<String, Either2> {
        @Override
        public Either2 map(String value) {
            return null;
        }
    }

    public static class EitherMapper3 implements MapFunction<Either2, Either2> {
        @Override
        public Either2 map(Either2 value) {
            return null;
        }
    }

    @Test
    void testEither() {
        MapFunction<?, ?> function =
                (MapFunction<Either<String, Boolean>, Either<String, Boolean>>) value -> null;
        TypeInformation<?> expected =
                new EitherTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO);
        TypeInformation<?> ti = TypeExtractor.getMapReturnTypes((MapFunction) function, expected);
        Assertions.assertEquals(expected, ti);
    }

    @Test
    void testEitherHierarchy() {
        MapFunction<?, ?> function = new EitherMapper<Boolean>();
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) function, BasicTypeInfo.BOOLEAN_TYPE_INFO);
        TypeInformation<?> expected =
                new EitherTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO);
        Assertions.assertEquals(expected, ti);

        function = new EitherMapper2();
        ti =
                TypeExtractor.getMapReturnTypes(
                        (MapFunction) function, BasicTypeInfo.STRING_TYPE_INFO);
        expected =
                new EitherTypeInfo<>(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new TupleTypeInfo(BasicTypeInfo.INT_TYPE_INFO));
        Assertions.assertEquals(expected, ti);

        function = new EitherMapper3();
        ti = TypeExtractor.getMapReturnTypes((MapFunction) function, expected);
        Assertions.assertEquals(expected, ti);

        Either<String, Tuple1<Integer>> either = new Either2();
        ti = TypeExtractor.getForObject(either);
        Assertions.assertEquals(expected, ti);
    }

    @Test(expected = InvalidTypesException.class)
     void testEitherFromObjectException() {
        Either<String, Tuple1<Integer>> either = Either.Left("test");
        TypeExtractor.getForObject(either);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testGenericTypeWithSubclassInput() {
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("a", "b");
        TypeInformation<?> inputType = TypeExtractor.createTypeInfo(inputMap.getClass());

        MapFunction<?, ?> function =
                (MapFunction<Map<String, Object>, Map<String, Object>>)
                        stringObjectMap -> stringObjectMap;

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(function, (TypeInformation) inputType);
        TypeInformation<?> expected = TypeExtractor.createTypeInfo(Map.class);
        Assertions.assertEquals(expected, ti);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expected = InvalidTypesException.class)
     void testGenericTypeWithSuperclassInput() {
        TypeInformation<?> inputType = TypeExtractor.createTypeInfo(Map.class);

        MapFunction<?, ?> function =
                (MapFunction<HashMap<String, Object>, Map<String, Object>>)
                        stringObjectMap -> stringObjectMap;

        TypeExtractor.getMapReturnTypes(function, (TypeInformation) inputType);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testInputWithCustomTypeInfo() {
        TypeInformation<?> customTypeInfo =
                new TypeInformation<Object>() {

                    @Override
                    public boolean isBasicType() {
                        return true;
                    }

                    @Override
                    public boolean isTupleType() {
                        return true;
                    }

                    @Override
                    public int getArity() {
                        return 0;
                    }

                    @Override
                    public int getTotalFields() {
                        return 0;
                    }

                    @Override
                    public Class getTypeClass() {
                        return null;
                    }

                    @Override
                    public boolean isKeyType() {
                        return false;
                    }

                    @Override
                    public TypeSerializer<Object> createSerializer(ExecutionConfig config) {
                        return null;
                    }

                    @Override
                    public String toString() {
                        return null;
                    }

                    @Override
                    public boolean equals(Object obj) {
                        return false;
                    }

                    @Override
                    public int hashCode() {
                        return 0;
                    }

                    @Override
                    public boolean canEqual(Object obj) {
                        return false;
                    }
                };

        MapFunction<?, ?> function = (MapFunction<Tuple1<String>, Tuple1<Object>>) value -> null;

        TypeExtractor.getMapReturnTypes(
                function, (TypeInformation) new TupleTypeInfo<Tuple1<Object>>(customTypeInfo));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testBigBasicTypes() {
        MapFunction<?, ?> function =
                (MapFunction<Tuple2<BigInteger, BigDecimal>, Tuple2<BigInteger, BigDecimal>>)
                        value -> null;

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(
                                        new TypeHint<Tuple2<BigInteger, BigDecimal>>() {}));

        Assertions.assertTrue(ti.isTupleType());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertEquals(BasicTypeInfo.BIG_INT_TYPE_INFO, tti.getTypeAt(0));
        Assertions.assertEquals(BasicTypeInfo.BIG_DEC_TYPE_INFO, tti.getTypeAt(1));

        // use getForClass()
        Assertions.assertTrue(TypeExtractor.getForClass(BigInteger.class).isBasicType());
        Assertions.assertTrue(TypeExtractor.getForClass(BigDecimal.class).isBasicType());
        Assertions.assertEquals(tti.getTypeAt(0), TypeExtractor.getForClass(BigInteger.class));
        Assertions.assertEquals(tti.getTypeAt(1), TypeExtractor.getForClass(BigDecimal.class));

        // use getForObject()
        Assertions.assertEquals(
                BasicTypeInfo.BIG_INT_TYPE_INFO, TypeExtractor.getForObject(new BigInteger("42")));
        Assertions.assertEquals(
                BasicTypeInfo.BIG_DEC_TYPE_INFO,
                TypeExtractor.getForObject(new BigDecimal("42.42")));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testSqlTimeTypes() {
        MapFunction<?, ?> function =
                (MapFunction<Tuple3<Date, Time, Timestamp>, Tuple3<Date, Time, Timestamp>>)
                        value -> null;

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        (TypeInformation)
                                TypeInformation.of(
                                        new TypeHint<Tuple3<Date, Time, Timestamp>>() {}));

        Assertions.assertTrue(ti.isTupleType());
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        Assertions.assertEquals(SqlTimeTypeInfo.DATE, tti.getTypeAt(0));
        Assertions.assertEquals(SqlTimeTypeInfo.TIME, tti.getTypeAt(1));
        Assertions.assertEquals(SqlTimeTypeInfo.TIMESTAMP, tti.getTypeAt(2));

        // use getForClass()
        Assertions.assertEquals(tti.getTypeAt(0), TypeExtractor.getForClass(Date.class));
        Assertions.assertEquals(tti.getTypeAt(1), TypeExtractor.getForClass(Time.class));
        Assertions.assertEquals(tti.getTypeAt(2), TypeExtractor.getForClass(Timestamp.class));

        // use getForObject()
        Assertions.assertEquals(
                SqlTimeTypeInfo.DATE, TypeExtractor.getForObject(Date.valueOf("1998-12-12")));
        Assertions.assertEquals(
                SqlTimeTypeInfo.TIME, TypeExtractor.getForObject(Time.valueOf("12:37:45")));
        Assertions.assertEquals(
                SqlTimeTypeInfo.TIMESTAMP,
                TypeExtractor.getForObject(Timestamp.valueOf("1998-12-12 12:37:45")));
    }
}

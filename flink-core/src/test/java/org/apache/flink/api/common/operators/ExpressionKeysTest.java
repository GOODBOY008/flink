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

package org.apache.flink.api.common.operators;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.operators.Keys.ExpressionKeys;
import org.apache.flink.api.common.operators.SelectorFunctionKeysTest.KeySelector1;
import org.apache.flink.api.common.operators.SelectorFunctionKeysTest.KeySelector3;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeExtractionTest.ComplexNestedClass;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("unused")
public class ExpressionKeysTest {

    @Test
    void testBasicType() {

        TypeInformation<Long> longType = BasicTypeInfo.LONG_TYPE_INFO;
        ExpressionKeys<Long> ek = new ExpressionKeys<>("*", longType);

        assertThat(ek.computeLogicalKeyPositions()).isEqualTo(new int[] {0});
    }

    @Test
    void testGenericNonKeyType() {
        // Fail: GenericType cannot be used as key
        TypeInformation<GenericNonKeyType> genericType =
                new GenericTypeInfo<>(GenericNonKeyType.class);
        assertThrows(InvalidProgramException.class, () -> new ExpressionKeys<>("*", genericType));
    }

    @Test
    void testKeyGenericType() {

        TypeInformation<GenericKeyType> genericType = new GenericTypeInfo<>(GenericKeyType.class);
        ExpressionKeys<GenericKeyType> ek = new ExpressionKeys<>("*", genericType);

        assertThat(ek.computeLogicalKeyPositions()).isEqualTo(new int[] {0});
    }

    @Test
    void testTupleRangeCheck() throws IllegalArgumentException {

        // valid indexes
        Keys.rangeCheckFields(new int[] {1, 2, 3, 4}, 4);

        // corner case tests
        Keys.rangeCheckFields(new int[] {0}, 0);

        Throwable ex = null;
        try {
            // throws illegal argument.
            Keys.rangeCheckFields(new int[] {5}, 0);
        } catch (Throwable iae) {
            ex = iae;
        }
        assertThat(ex).isNotNull();
    }

    @Test
    void testStandardTupleKeys() {
        TupleTypeInfo<Tuple7<String, String, String, String, String, String, String>> typeInfo =
                new TupleTypeInfo<>(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        ExpressionKeys<Tuple7<String, String, String, String, String, String, String>> ek;

        for (int i = 1; i < 8; i++) {
            int[] ints = new int[i];
            for (int j = 0; j < i; j++) {
                ints[j] = j;
            }
            int[] inInts =
                    Arrays.copyOf(
                            ints,
                            ints.length); // copy, just to make sure that the code is not cheating
            // by changing the ints.
            ek = new ExpressionKeys<>(inInts, typeInfo);
            assertThat(ek.computeLogicalKeyPositions()).isEqualTo(ints);
            assertThat(ek.computeLogicalKeyPositions().length).isEqualTo(ints.length);

            ArrayUtils.reverse(ints);
            inInts = Arrays.copyOf(ints, ints.length);
            ek = new ExpressionKeys<>(inInts, typeInfo);
            assertThat(ek.computeLogicalKeyPositions()).isEqualTo(ints);
            assertThat(ek.computeLogicalKeyPositions().length).isEqualTo(ints.length);
        }
    }

    @Test
    void testInvalidTuple() {
        TupleTypeInfo<Tuple3<String, Tuple3<String, String, String>, String>> typeInfo =
                new TupleTypeInfo<>(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new TupleTypeInfo<Tuple3<String, String, String>>(
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO),
                        BasicTypeInfo.STRING_TYPE_INFO);

        String[][] tests =
                new String[][] {
                    new String[] {"f0.f1"}, // nesting into unnested
                    new String[] {"f11"},
                    new String[] {"f-35"},
                    new String[] {"f0.f33"},
                    new String[] {"f1.f33"}
                };
        for (String[] test : tests) {
            Throwable e = null;
            try {
                new ExpressionKeys<>(test, typeInfo);
            } catch (Throwable t) {
                e = t;
            }
            assertThat(e).isNotNull();
        }
    }

    @Test
    void testTupleNonKeyField() {
        // selected field is not a key type
        TypeInformation<Tuple3<String, Long, GenericNonKeyType>> ti =
                new TupleTypeInfo<>(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        TypeExtractor.getForClass(GenericNonKeyType.class));

        assertThrows(InvalidProgramException.class, () -> new ExpressionKeys<>(2, ti));
    }

    @Test
    void testTupleKeyExpansion() {
        TupleTypeInfo<Tuple3<String, Tuple3<String, String, String>, String>> typeInfo =
                new TupleTypeInfo<>(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new TupleTypeInfo<Tuple3<String, String, String>>(
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO),
                        BasicTypeInfo.STRING_TYPE_INFO);
        ExpressionKeys<Tuple3<String, Tuple3<String, String, String>, String>> fpk =
                new ExpressionKeys<>(0, typeInfo);
        assertThat(fpk.computeLogicalKeyPositions()).isEqualTo(new int[] {0});

        fpk = new ExpressionKeys<>(1, typeInfo);
        Assertions.assertArrayEquals(new int[] {1, 2, 3}, fpk.computeLogicalKeyPositions());

        fpk = new ExpressionKeys<>(2, typeInfo);
        assertThat(fpk.computeLogicalKeyPositions()).isEqualTo(new int[] {4});

        fpk = new ExpressionKeys<>(new int[] {0, 1, 2}, typeInfo);
        Assertions.assertArrayEquals(new int[] {0, 1, 2, 3, 4}, fpk.computeLogicalKeyPositions());

        fpk = new ExpressionKeys<>(null, typeInfo, true); // empty case
        Assertions.assertArrayEquals(new int[] {0, 1, 2, 3, 4}, fpk.computeLogicalKeyPositions());

        fpk = new ExpressionKeys<>("*", typeInfo);
        Assertions.assertArrayEquals(new int[] {0, 1, 2, 3, 4}, fpk.computeLogicalKeyPositions());

        // scala style "select all"
        fpk = new ExpressionKeys<>("_", typeInfo);
        Assertions.assertArrayEquals(new int[] {0, 1, 2, 3, 4}, fpk.computeLogicalKeyPositions());

        // this was a bug:
        fpk = new ExpressionKeys<>("f2", typeInfo);
        Assertions.assertArrayEquals(new int[] {4}, fpk.computeLogicalKeyPositions());

        fpk = new ExpressionKeys<>(new String[] {"f0", "f1.f0", "f1.f1", "f1.f2", "f2"}, typeInfo);
        Assertions.assertArrayEquals(new int[] {0, 1, 2, 3, 4}, fpk.computeLogicalKeyPositions());

        fpk = new ExpressionKeys<>(new String[] {"f0", "f1.f0", "f1.f1", "f2"}, typeInfo);
        Assertions.assertArrayEquals(new int[] {0, 1, 2, 4}, fpk.computeLogicalKeyPositions());

        fpk = new ExpressionKeys<>(new String[] {"f2", "f0"}, typeInfo);
        Assertions.assertArrayEquals(new int[] {4, 0}, fpk.computeLogicalKeyPositions());

        TupleTypeInfo<
                        Tuple3<
                                String,
                                Tuple3<Tuple3<String, String, String>, String, String>,
                                String>>
                complexTypeInfo =
                        new TupleTypeInfo<>(
                                BasicTypeInfo.STRING_TYPE_INFO,
                                new TupleTypeInfo<
                                        Tuple3<Tuple3<String, String, String>, String, String>>(
                                        new TupleTypeInfo<Tuple3<String, String, String>>(
                                                BasicTypeInfo.STRING_TYPE_INFO,
                                                BasicTypeInfo.STRING_TYPE_INFO,
                                                BasicTypeInfo.STRING_TYPE_INFO),
                                        BasicTypeInfo.STRING_TYPE_INFO,
                                        BasicTypeInfo.STRING_TYPE_INFO),
                                BasicTypeInfo.STRING_TYPE_INFO);

        ExpressionKeys<
                        Tuple3<
                                String,
                                Tuple3<Tuple3<String, String, String>, String, String>,
                                String>>
                complexFpk = new ExpressionKeys<>(0, complexTypeInfo);
        assertThat(complexFpk.computeLogicalKeyPositions()).isEqualTo(new int[] {0});

        complexFpk = new ExpressionKeys<>(new int[] {0, 1, 2}, complexTypeInfo);
        assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6}, complexFpk.computeLogicalKeyPositions());

        complexFpk = new ExpressionKeys<>("*", complexTypeInfo);
        assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6}, complexFpk.computeLogicalKeyPositions());

        // scala style select all
        complexFpk = new ExpressionKeys<>("_", complexTypeInfo);
        assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6}, complexFpk.computeLogicalKeyPositions());

        complexFpk = new ExpressionKeys<>("f1.f0.*", complexTypeInfo);
        Assertions.assertArrayEquals(new int[] {1, 2, 3}, complexFpk.computeLogicalKeyPositions());

        complexFpk = new ExpressionKeys<>("f1.f0", complexTypeInfo);
        Assertions.assertArrayEquals(new int[] {1, 2, 3}, complexFpk.computeLogicalKeyPositions());

        complexFpk = new ExpressionKeys<>("f2", complexTypeInfo);
        assertThat(complexFpk.computeLogicalKeyPositions()).isEqualTo(new int[] {6});
    }

    @Test
    void testPojoKeys() {
        TypeInformation<PojoWithMultiplePojos> ti =
                TypeExtractor.getForClass(PojoWithMultiplePojos.class);
        ExpressionKeys<PojoWithMultiplePojos> ek;
        ek = new ExpressionKeys<>("*", ti);
        Assertions.assertArrayEquals(new int[] {0, 1, 2, 3, 4}, ek.computeLogicalKeyPositions());

        ek = new ExpressionKeys<>("p1.*", ti);
        Assertions.assertArrayEquals(new int[] {1, 2}, ek.computeLogicalKeyPositions());

        ek = new ExpressionKeys<>("p2.*", ti);
        Assertions.assertArrayEquals(new int[] {3, 4}, ek.computeLogicalKeyPositions());

        ek = new ExpressionKeys<>("p1", ti);
        Assertions.assertArrayEquals(new int[] {1, 2}, ek.computeLogicalKeyPositions());

        ek = new ExpressionKeys<>("p2", ti);
        Assertions.assertArrayEquals(new int[] {3, 4}, ek.computeLogicalKeyPositions());

        ek = new ExpressionKeys<>("i0", ti);
        assertThat(ek.computeLogicalKeyPositions()).isEqualTo(new int[] {0});
    }

    @Test
    void testTupleWithNestedPojo() {

        TypeInformation<Tuple3<Integer, Pojo1, PojoWithMultiplePojos>> ti =
                new TupleTypeInfo<>(
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeExtractor.getForClass(Pojo1.class),
                        TypeExtractor.getForClass(PojoWithMultiplePojos.class));

        ExpressionKeys<Tuple3<Integer, Pojo1, PojoWithMultiplePojos>> ek;

        ek = new ExpressionKeys<>(0, ti);
        assertThat(ek.computeLogicalKeyPositions()).isEqualTo(new int[] {0});

        ek = new ExpressionKeys<>(1, ti);
        Assertions.assertArrayEquals(new int[] {1, 2}, ek.computeLogicalKeyPositions());

        ek = new ExpressionKeys<>(2, ti);
        Assertions.assertArrayEquals(new int[] {3, 4, 5, 6, 7}, ek.computeLogicalKeyPositions());

        ek = new ExpressionKeys<>(new int[] {}, ti, true);
        assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6, 7}, ek.computeLogicalKeyPositions());

        ek = new ExpressionKeys<>("*", ti);
        assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6, 7}, ek.computeLogicalKeyPositions());

        ek = new ExpressionKeys<>("f2.p1.*", ti);
        Assertions.assertArrayEquals(new int[] {4, 5}, ek.computeLogicalKeyPositions());
    }

    @Test
    void testOriginalTypes() {

        TypeInformation<Tuple3<Integer, Pojo1, PojoWithMultiplePojos>> ti =
                new TupleTypeInfo<>(
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeExtractor.getForClass(Pojo1.class),
                        TypeExtractor.getForClass(PojoWithMultiplePojos.class));

        ExpressionKeys<Tuple3<Integer, Pojo1, PojoWithMultiplePojos>> ek;

        ek = new ExpressionKeys<>(0, ti);
        assertArrayEquals(
                new TypeInformation[] {BasicTypeInfo.INT_TYPE_INFO}, ek.getOriginalKeyFieldTypes());

        ek = new ExpressionKeys<>(1, ti);
        assertArrayEquals(
                new TypeInformation[] {TypeExtractor.getForClass(Pojo1.class)},
                ek.getOriginalKeyFieldTypes());

        ek = new ExpressionKeys<>(2, ti);
        assertArrayEquals(
                new TypeInformation[] {TypeExtractor.getForClass(PojoWithMultiplePojos.class)},
                ek.getOriginalKeyFieldTypes());

        ek = new ExpressionKeys<>(new int[] {}, ti, true);
        assertArrayEquals(
                new TypeInformation<?>[] {
                    BasicTypeInfo.INT_TYPE_INFO,
                    TypeExtractor.getForClass(Pojo1.class),
                    TypeExtractor.getForClass(PojoWithMultiplePojos.class)
                },
                ek.getOriginalKeyFieldTypes());

        ek = new ExpressionKeys<>("*", ti);
        assertThat(ek.getOriginalKeyFieldTypes()).isEqualTo(new TypeInformation<?>[] {ti});

        ek = new ExpressionKeys<>("f1", ti);
        assertArrayEquals(
                new TypeInformation<?>[] {TypeExtractor.getForClass(Pojo1.class)},
                ek.getOriginalKeyFieldTypes());

        ek = new ExpressionKeys<>("f1.*", ti);
        assertArrayEquals(
                new TypeInformation<?>[] {TypeExtractor.getForClass(Pojo1.class)},
                ek.getOriginalKeyFieldTypes());

        ek = new ExpressionKeys<>("f2.*", ti);
        assertArrayEquals(
                new TypeInformation<?>[] {TypeExtractor.getForClass(PojoWithMultiplePojos.class)},
                ek.getOriginalKeyFieldTypes());

        ek = new ExpressionKeys<>("f2.p2", ti);
        assertArrayEquals(
                new TypeInformation<?>[] {TypeExtractor.getForClass(Pojo2.class)},
                ek.getOriginalKeyFieldTypes());

        ek = new ExpressionKeys<>("f2.p2.*", ti);
        assertArrayEquals(
                new TypeInformation<?>[] {TypeExtractor.getForClass(Pojo2.class)},
                ek.getOriginalKeyFieldTypes());

        ek = new ExpressionKeys<>("f2.p2._", ti);
        assertArrayEquals(
                new TypeInformation<?>[] {TypeExtractor.getForClass(Pojo2.class)},
                ek.getOriginalKeyFieldTypes());
    }

    @Test
    void testNonKeyPojoField() {
        // selected field is not a key type
        TypeInformation<PojoWithNonKeyField> ti =
                TypeExtractor.getForClass(PojoWithNonKeyField.class);
        assertThatThrownBy(() -> new ExpressionKeys<>("b", ti))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testInvalidPojo() {
        TypeInformation<ComplexNestedClass> ti =
                TypeExtractor.getForClass(ComplexNestedClass.class);

        String[][] tests =
                new String[][] {
                    new String[] {"nonexistent"}, new String[] {"date.abc"} // nesting into unnested
                };
        for (String[] test : tests) {
            Throwable e = null;
            try {
                new ExpressionKeys<>(test, ti);
            } catch (Throwable t) {
                e = t;
            }
            assertThat(e).isNotNull();
        }
    }

    @Test
    void testAreCompatible1() throws Keys.IncompatibleKeysException {
        TypeInformation<Pojo1> t1 = TypeExtractor.getForClass(Pojo1.class);

        ExpressionKeys<Pojo1> ek1 = new ExpressionKeys<>("a", t1);
        ExpressionKeys<Pojo1> ek2 = new ExpressionKeys<>("b", t1);

        assertThat(ek1.areCompatible(ek2)).isTrue();
        assertThat(ek2.areCompatible(ek1)).isTrue();
    }

    @Test
    void testAreCompatible2() throws Keys.IncompatibleKeysException {
        TypeInformation<Pojo1> t1 = TypeExtractor.getForClass(Pojo1.class);
        TypeInformation<Tuple2<String, Long>> t2 =
                new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

        ExpressionKeys<Pojo1> ek1 = new ExpressionKeys<>("a", t1);
        ExpressionKeys<Tuple2<String, Long>> ek2 = new ExpressionKeys<>(0, t2);

        assertThat(ek1.areCompatible(ek2)).isTrue();
        assertThat(ek2.areCompatible(ek1)).isTrue();
    }

    @Test
    void testAreCompatible3() throws Keys.IncompatibleKeysException {
        TypeInformation<String> t1 = BasicTypeInfo.STRING_TYPE_INFO;
        TypeInformation<Tuple2<String, Long>> t2 =
                new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

        ExpressionKeys<String> ek1 = new ExpressionKeys<>("*", t1);
        ExpressionKeys<Tuple2<String, Long>> ek2 = new ExpressionKeys<>(0, t2);

        assertThat(ek1.areCompatible(ek2)).isTrue();
        assertThat(ek2.areCompatible(ek1)).isTrue();
    }

    @Test
    void testAreCompatible4() throws Keys.IncompatibleKeysException {
        TypeInformation<PojoWithMultiplePojos> t1 =
                TypeExtractor.getForClass(PojoWithMultiplePojos.class);
        TypeInformation<Tuple3<String, Long, Integer>> t2 =
                new TupleTypeInfo<>(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);

        ExpressionKeys<PojoWithMultiplePojos> ek1 =
                new ExpressionKeys<>(new String[] {"p1", "i0"}, t1);
        ExpressionKeys<Tuple3<String, Long, Integer>> ek2 =
                new ExpressionKeys<>(new int[] {0, 0, 2}, t2);

        assertThat(ek1.areCompatible(ek2)).isTrue();
        assertThat(ek2.areCompatible(ek1)).isTrue();
    }

    @Test
    void testAreCompatible5() throws Keys.IncompatibleKeysException {
        TypeInformation<PojoWithMultiplePojos> t1 =
                TypeExtractor.getForClass(PojoWithMultiplePojos.class);
        TypeInformation<Tuple2<String, String>> t2 =
                new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        ExpressionKeys<PojoWithMultiplePojos> ek1 =
                new ExpressionKeys<>(new String[] {"p1.b", "p2.a2"}, t1);
        ExpressionKeys<Tuple2<String, String>> ek2 = new ExpressionKeys<>("*", t2);

        assertThat(ek1.areCompatible(ek2)).isTrue();
        assertThat(ek2.areCompatible(ek1)).isTrue();
    }

    @Test
    void testAreCompatible6() throws Keys.IncompatibleKeysException {
        TypeInformation<Pojo1> t1 = TypeExtractor.getForClass(Pojo1.class);
        TypeInformation<Tuple2<String, Long>> t2 =
                new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

        ExpressionKeys<Pojo1> ek1 = new ExpressionKeys<>("a", t1);
        ExpressionKeys<Tuple2<String, Long>> ek2 = new ExpressionKeys<>(1, t2);

        assertThrows(Keys.IncompatibleKeysException.class, () -> ek1.areCompatible(ek2));
    }

    @Test
    void testAreCompatible7() throws Keys.IncompatibleKeysException {
        TypeInformation<Pojo1> t1 = TypeExtractor.getForClass(Pojo1.class);
        TypeInformation<Tuple2<String, Long>> t2 =
                new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

        ExpressionKeys<Pojo1> ek1 = new ExpressionKeys<>(new String[] {"a", "b"}, t1);
        ExpressionKeys<Tuple2<String, Long>> ek2 = new ExpressionKeys<>(0, t2);

        assertThrows(Keys.IncompatibleKeysException.class, () -> ek1.areCompatible(ek2));
    }

    @Test
    void testAreCompatible8() throws Keys.IncompatibleKeysException {
        TypeInformation<String> t1 = BasicTypeInfo.STRING_TYPE_INFO;
        TypeInformation<Pojo2> t2 = TypeExtractor.getForClass(Pojo2.class);

        ExpressionKeys<String> ek1 = new ExpressionKeys<>("*", t1);
        Keys<Pojo2> ek2 =
                new Keys.SelectorFunctionKeys<>(
                        new KeySelector1(), t2, BasicTypeInfo.STRING_TYPE_INFO);

        assertThat(ek1.areCompatible(ek2)).isTrue();
    }

    @Test
    void testAreCompatible9() throws Keys.IncompatibleKeysException {
        TypeInformation<Tuple3<String, Long, Integer>> t1 =
                new TupleTypeInfo<>(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);
        TypeInformation<PojoWithMultiplePojos> t2 =
                TypeExtractor.getForClass(PojoWithMultiplePojos.class);

        ExpressionKeys<Tuple3<String, Long, Integer>> ek1 =
                new ExpressionKeys<>(new int[] {2, 0}, t1);
        Keys<PojoWithMultiplePojos> ek2 =
                new Keys.SelectorFunctionKeys<>(
                        new KeySelector3(),
                        t2,
                        new TupleTypeInfo<>(
                                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

        assertThat(ek1.areCompatible(ek2)).isTrue();
    }

    public static class Pojo1 {
        public String a;
        public String b;
    }

    public static class Pojo2 {
        public String a2;
        public String b2;
    }

    public static class PojoWithMultiplePojos {
        public Pojo1 p1;
        public Pojo2 p2;
        public Integer i0;
    }

    public static class PojoWithNonKeyField {
        public String a;
        public GenericNonKeyType b;
    }

    public static class GenericNonKeyType {
        private String a;
        private String b;
    }

    public static class GenericKeyType implements Comparable<GenericNonKeyType> {
        private String a;
        private String b;

        @Override
        public int compareTo(GenericNonKeyType o) {
            return 0;
        }
    }
}

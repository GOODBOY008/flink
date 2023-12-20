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

import org.apache.flink.api.common.operators.SemanticProperties.InvalidSemanticAnnotationException;
import org.apache.flink.api.common.operators.util.FieldSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.within;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SingleInputSemanticPropertiesTest {

    @Test
    void testGetTargetFields() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 1);
        sp.addForwardedField(1, 4);
        sp.addForwardedField(2, 3);
        sp.addForwardedField(3, 2);

        assertThat(sp.getForwardingTargetFields(0, 0)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 1)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 2)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 3)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4)).isNotNull();
        assertThat(sp.getForwardingTargetFields(0, 4)).isEmpty();

        sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 0);
        sp.addForwardedField(0, 4);
        sp.addForwardedField(1, 1);
        sp.addForwardedField(1, 2);
        sp.addForwardedField(1, 3);

        assertThat(sp.getForwardingTargetFields(0, 0)).hasSize(2);
        assertThat(sp.getForwardingTargetFields(0, 1)).hasSize(3);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2)).isNotNull();
        assertThat(sp.getForwardingTargetFields(0, 2)).isEmpty();
    }

    @Test
    void testGetSourceField() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 1);
        sp.addForwardedField(1, 4);
        sp.addForwardedField(2, 3);
        sp.addForwardedField(3, 2);

        assertThat(sp.getForwardingSourceField(0).isCloseTo(0, within(1)));
        assertThat(sp.getForwardingSourceField(0).isCloseTo(1, within(4)));
        assertThat(sp.getForwardingSourceField(0).isCloseTo(2, within(3)));
        assertThat(sp.getForwardingSourceField(0).isCloseTo(3, within(2)));
        assertTrue(sp.getForwardingSourceField(0, 0) < 0);
        assertTrue(sp.getForwardingSourceField(0, 5) < 0);

        sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 0);
        sp.addForwardedField(0, 4);
        sp.addForwardedField(1, 1);
        sp.addForwardedField(1, 2);
        sp.addForwardedField(1, 3);

        assertThat(sp.getForwardingSourceField(0).isCloseTo(0, within(0)));
        assertThat(sp.getForwardingSourceField(0).isCloseTo(0, within(4)));
        assertThat(sp.getForwardingSourceField(0).isCloseTo(1, within(1)));
        assertThat(sp.getForwardingSourceField(0).isCloseTo(1, within(2)));
        assertThat(sp.getForwardingSourceField(0).isCloseTo(1, within(3)));
        assertTrue(sp.getForwardingSourceField(0, 5) < 0);
    }

    @Test
    void testGetReadSet() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addReadFields(new FieldSet(0, 1));

        assertThat(sp.getReadFields(0)).hasSize(2);
        assertThat(sp.getReadFields(0).contains(0)).isTrue();
        assertThat(sp.getReadFields(0).contains(1)).isTrue();

        sp.addReadFields(new FieldSet(3));

        assertThat(sp.getReadFields(0)).hasSize(3);
        assertThat(sp.getReadFields(0).contains(0)).isTrue();
        assertThat(sp.getReadFields(0).contains(1)).isTrue();
        assertThat(sp.getReadFields(0).contains(3)).isTrue();
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    void testAddForwardedFieldsTargetTwice() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 2);
        sp.addForwardedField(1, 2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    void testGetTargetFieldInvalidIndex() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 0);

        sp.getForwardingTargetFields(1, 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    void testGetSourceFieldInvalidIndex() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 0);

        sp.getForwardingSourceField(1, 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    void testGetReadFieldsInvalidIndex() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addReadFields(new FieldSet(0, 1));

        sp.getReadFields(1);
    }

    @Test
    void testAllForwardedSingleInputSemProps() {

        SingleInputSemanticProperties sp =
                new SingleInputSemanticProperties.AllFieldsForwardedProperties();

        assertThat(sp.getForwardingTargetFields(0, 0)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 1)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 123)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 123).contains(123)).isTrue();

        assertThat(sp.getForwardingSourceField(0).isCloseTo(0, within(0)));
        assertThat(sp.getForwardingSourceField(0).isCloseTo(2, within(2)));
        assertThat(sp.getForwardingSourceField(0).isCloseTo(123, within(123)));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    void testAllForwardedSingleInputSemPropsInvalidIndex1() {

        SingleInputSemanticProperties sp =
                new SingleInputSemanticProperties.AllFieldsForwardedProperties();
        sp.getForwardingSourceField(1, 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    void testAllForwardedSingleInputSemPropsInvalidIndex2() {

        SingleInputSemanticProperties sp =
                new SingleInputSemanticProperties.AllFieldsForwardedProperties();
        sp.getForwardingTargetFields(1, 0);
    }
}

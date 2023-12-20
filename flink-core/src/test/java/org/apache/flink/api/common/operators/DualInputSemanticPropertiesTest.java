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

import org.apache.flink.api.common.operators.util.FieldSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DualInputSemanticPropertiesTest {

    @Test
    void testGetTargetFields() {

        // first input
        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 1);
        sp.addForwardedField(0, 1, 4);
        sp.addForwardedField(0, 2, 3);
        sp.addForwardedField(0, 3, 2);

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

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 0);
        sp.addForwardedField(0, 0, 4);
        sp.addForwardedField(0, 1, 1);
        sp.addForwardedField(0, 1, 2);
        sp.addForwardedField(0, 1, 3);

        assertThat(sp.getForwardingTargetFields(0, 0)).hasSize(2);
        assertThat(sp.getForwardingTargetFields(0, 1)).hasSize(3);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2)).isNotNull();
        assertThat(sp.getForwardingTargetFields(0, 2)).isEmpty();

        // second input
        sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 1);
        sp.addForwardedField(1, 1, 4);
        sp.addForwardedField(1, 2, 3);
        sp.addForwardedField(1, 3, 2);

        assertThat(sp.getForwardingTargetFields(1, 0)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(1, 1)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(1, 2)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(1, 3)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(1, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 2).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 3).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 4)).isNotNull();
        assertThat(sp.getForwardingTargetFields(1, 4)).isEmpty();

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 0);
        sp.addForwardedField(1, 0, 4);
        sp.addForwardedField(1, 1, 1);
        sp.addForwardedField(1, 1, 2);
        sp.addForwardedField(1, 1, 3);

        assertThat(sp.getForwardingTargetFields(1, 0)).hasSize(2);
        assertThat(sp.getForwardingTargetFields(1, 1)).hasSize(3);
        assertThat(sp.getForwardingTargetFields(1, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 0).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 2)).isNotNull();
        assertThat(sp.getForwardingTargetFields(1, 2)).isEmpty();

        // both inputs
        sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 2, 6);
        sp.addForwardedField(0, 7, 8);
        sp.addForwardedField(1, 0, 1);
        sp.addForwardedField(1, 1, 4);

        assertThat(sp.getForwardingTargetFields(0, 2)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 7)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(1, 0)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(1, 1)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 2).contains(6)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 7).contains(8)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1)).isNotNull();
        assertThat(sp.getForwardingTargetFields(1, 4)).isNotNull();
        assertThat(sp.getForwardingTargetFields(0, 1)).isEmpty();
        assertThat(sp.getForwardingTargetFields(1, 4)).isEmpty();

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 0);
        sp.addForwardedField(0, 0, 4);
        sp.addForwardedField(0, 3, 8);
        sp.addForwardedField(1, 1, 1);
        sp.addForwardedField(1, 1, 2);
        sp.addForwardedField(1, 4, 8);

        assertThat(sp.getForwardingTargetFields(0, 0)).hasSize(2);
        assertThat(sp.getForwardingTargetFields(0, 3)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(1, 1)).hasSize(2);
        assertThat(sp.getForwardingTargetFields(1, 4)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(8)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 4).contains(8)).isTrue();
    }

    @Test
    void testGetSourceField() {

        // first input
        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 1);
        sp.addForwardedField(0, 1, 4);
        sp.addForwardedField(0, 2, 3);
        sp.addForwardedField(0, 3, 2);

        assertEquals(0, sp.getForwardingSourceField(0, 1));
        assertEquals(1, sp.getForwardingSourceField(0, 4));
        assertEquals(2, sp.getForwardingSourceField(0, 3));
        assertEquals(3, sp.getForwardingSourceField(0, 2));
        assertTrue(sp.getForwardingSourceField(0, 0) < 0);
        assertTrue(sp.getForwardingSourceField(0, 5) < 0);

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 0);
        sp.addForwardedField(0, 0, 4);
        sp.addForwardedField(0, 1, 1);
        sp.addForwardedField(0, 1, 2);
        sp.addForwardedField(0, 1, 3);

        assertEquals(0, sp.getForwardingSourceField(0, 0));
        assertEquals(0, sp.getForwardingSourceField(0, 4));
        assertEquals(1, sp.getForwardingSourceField(0, 1));
        assertEquals(1, sp.getForwardingSourceField(0, 2));
        assertEquals(1, sp.getForwardingSourceField(0, 3));
        assertTrue(sp.getForwardingSourceField(0, 5) < 0);

        // second input
        sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 1);
        sp.addForwardedField(1, 1, 4);
        sp.addForwardedField(1, 2, 3);
        sp.addForwardedField(1, 3, 2);

        assertEquals(0, sp.getForwardingSourceField(1, 1));
        assertEquals(1, sp.getForwardingSourceField(1, 4));
        assertEquals(2, sp.getForwardingSourceField(1, 3));
        assertEquals(3, sp.getForwardingSourceField(1, 2));
        assertTrue(sp.getForwardingSourceField(1, 0) < 0);
        assertTrue(sp.getForwardingSourceField(1, 5) < 0);

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 0);
        sp.addForwardedField(1, 0, 4);
        sp.addForwardedField(1, 1, 1);
        sp.addForwardedField(1, 1, 2);
        sp.addForwardedField(1, 1, 3);

        assertEquals(0, sp.getForwardingSourceField(1, 0));
        assertEquals(0, sp.getForwardingSourceField(1, 4));
        assertEquals(1, sp.getForwardingSourceField(1, 1));
        assertEquals(1, sp.getForwardingSourceField(1, 2));
        assertEquals(1, sp.getForwardingSourceField(1, 3));
        assertTrue(sp.getForwardingSourceField(1, 5) < 0);
    }

    @Test
    void testGetReadSet() {

        // first input
        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addReadFields(0, new FieldSet(0, 1));

        assertThat(sp.getReadFields(0)).hasSize(2);
        assertThat(sp.getReadFields(0).contains(0)).isTrue();
        assertThat(sp.getReadFields(0).contains(1)).isTrue();

        sp.addReadFields(0, new FieldSet(3));

        assertThat(sp.getReadFields(0)).hasSize(3);
        assertThat(sp.getReadFields(0).contains(0)).isTrue();
        assertThat(sp.getReadFields(0).contains(1)).isTrue();
        assertThat(sp.getReadFields(0).contains(3)).isTrue();

        // second input
        sp = new DualInputSemanticProperties();
        sp.addReadFields(1, new FieldSet(0, 1));

        assertThat(sp.getReadFields(1)).hasSize(2);
        assertThat(sp.getReadFields(1).contains(0)).isTrue();
        assertThat(sp.getReadFields(1).contains(1)).isTrue();

        sp.addReadFields(1, new FieldSet(3));

        assertThat(sp.getReadFields(1)).hasSize(3);
        assertThat(sp.getReadFields(1).contains(0)).isTrue();
        assertThat(sp.getReadFields(1).contains(1)).isTrue();
        assertThat(sp.getReadFields(1).contains(3)).isTrue();
    }

    @Test
    void testAddForwardedFieldsTargetTwice1() {

        assertThatThrownBy(
                        () -> {
                            DualInputSemanticProperties sp = new DualInputSemanticProperties();
                            sp.addForwardedField(0, 0, 2);
                            sp.addForwardedField(0, 1, 2);
                        })
                .isInstanceOf(SemanticProperties.InvalidSemanticAnnotationException.class);
    }

    @Test
    void testAddForwardedFieldsTargetTwice2() {

        assertThatThrownBy(
                        () -> {
                            DualInputSemanticProperties sp = new DualInputSemanticProperties();
                            sp.addForwardedField(1, 0, 2);
                            sp.addForwardedField(1, 1, 2);
                        })
                .isInstanceOf(SemanticProperties.InvalidSemanticAnnotationException.class);
    }
}

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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DualInputSemanticPropertiesTest {

    @Test
    void testGetTargetFields() {

        // first input
        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 1);
        sp.addForwardedField(0, 1, 4);
        sp.addForwardedField(0, 2, 3);
        sp.addForwardedField(0, 3, 2);

        Assertions.assertEquals(1, sp.getForwardingTargetFields(0, 0).size());
        Assertions.assertEquals(1, sp.getForwardingTargetFields(0, 1).size());
        Assertions.assertEquals(1, sp.getForwardingTargetFields(0, 2).size());
        Assertions.assertEquals(1, sp.getForwardingTargetFields(0, 3).size());
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 0).contains(1));
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 1).contains(4));
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 2).contains(3));
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 3).contains(2));
        Assertions.assertNotNull(sp.getForwardingTargetFields(0, 4));
        Assertions.assertEquals(0, sp.getForwardingTargetFields(0, 4).size());

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 0);
        sp.addForwardedField(0, 0, 4);
        sp.addForwardedField(0, 1, 1);
        sp.addForwardedField(0, 1, 2);
        sp.addForwardedField(0, 1, 3);

        Assertions.assertEquals(2, sp.getForwardingTargetFields(0, 0).size());
        Assertions.assertEquals(3, sp.getForwardingTargetFields(0, 1).size());
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 0).contains(0));
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 0).contains(4));
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 1).contains(1));
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 1).contains(2));
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 1).contains(3));
        Assertions.assertNotNull(sp.getForwardingTargetFields(0, 2));
        Assertions.assertEquals(0, sp.getForwardingTargetFields(0, 2).size());

        // second input
        sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 1);
        sp.addForwardedField(1, 1, 4);
        sp.addForwardedField(1, 2, 3);
        sp.addForwardedField(1, 3, 2);

        Assertions.assertEquals(1, sp.getForwardingTargetFields(1, 0).size());
        Assertions.assertEquals(1, sp.getForwardingTargetFields(1, 1).size());
        Assertions.assertEquals(1, sp.getForwardingTargetFields(1, 2).size());
        Assertions.assertEquals(1, sp.getForwardingTargetFields(1, 3).size());
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 0).contains(1));
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 1).contains(4));
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 2).contains(3));
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 3).contains(2));
        Assertions.assertNotNull(sp.getForwardingTargetFields(1, 4));
        Assertions.assertEquals(0, sp.getForwardingTargetFields(1, 4).size());

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 0);
        sp.addForwardedField(1, 0, 4);
        sp.addForwardedField(1, 1, 1);
        sp.addForwardedField(1, 1, 2);
        sp.addForwardedField(1, 1, 3);

        Assertions.assertEquals(2, sp.getForwardingTargetFields(1, 0).size());
        Assertions.assertEquals(3, sp.getForwardingTargetFields(1, 1).size());
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 0).contains(0));
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 0).contains(4));
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 1).contains(1));
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 1).contains(2));
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 1).contains(3));
        Assertions.assertNotNull(sp.getForwardingTargetFields(1, 2));
        Assertions.assertEquals(0, sp.getForwardingTargetFields(1, 2).size());

        // both inputs
        sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 2, 6);
        sp.addForwardedField(0, 7, 8);
        sp.addForwardedField(1, 0, 1);
        sp.addForwardedField(1, 1, 4);

        Assertions.assertEquals(1, sp.getForwardingTargetFields(0, 2).size());
        Assertions.assertEquals(1, sp.getForwardingTargetFields(0, 7).size());
        Assertions.assertEquals(1, sp.getForwardingTargetFields(1, 0).size());
        Assertions.assertEquals(1, sp.getForwardingTargetFields(1, 1).size());
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 2).contains(6));
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 7).contains(8));
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 0).contains(1));
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 1).contains(4));
        Assertions.assertNotNull(sp.getForwardingTargetFields(0, 1));
        Assertions.assertNotNull(sp.getForwardingTargetFields(1, 4));
        Assertions.assertEquals(0, sp.getForwardingTargetFields(0, 1).size());
        Assertions.assertEquals(0, sp.getForwardingTargetFields(1, 4).size());

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 0);
        sp.addForwardedField(0, 0, 4);
        sp.addForwardedField(0, 3, 8);
        sp.addForwardedField(1, 1, 1);
        sp.addForwardedField(1, 1, 2);
        sp.addForwardedField(1, 4, 8);

        Assertions.assertEquals(2, sp.getForwardingTargetFields(0, 0).size());
        Assertions.assertEquals(1, sp.getForwardingTargetFields(0, 3).size());
        Assertions.assertEquals(2, sp.getForwardingTargetFields(1, 1).size());
        Assertions.assertEquals(1, sp.getForwardingTargetFields(1, 4).size());
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 0).contains(0));
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 0).contains(4));
        Assertions.assertTrue(sp.getForwardingTargetFields(0, 3).contains(8));
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 1).contains(1));
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 1).contains(1));
        Assertions.assertTrue(sp.getForwardingTargetFields(1, 4).contains(8));
    }

    @Test
    void testGetSourceField() {

        // first input
        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 1);
        sp.addForwardedField(0, 1, 4);
        sp.addForwardedField(0, 2, 3);
        sp.addForwardedField(0, 3, 2);

        Assertions.assertEquals(0, sp.getForwardingSourceField(0, 1));
        Assertions.assertEquals(1, sp.getForwardingSourceField(0, 4));
        Assertions.assertEquals(2, sp.getForwardingSourceField(0, 3));
        Assertions.assertEquals(3, sp.getForwardingSourceField(0, 2));
        Assertions.assertTrue(sp.getForwardingSourceField(0, 0) < 0);
        Assertions.assertTrue(sp.getForwardingSourceField(0, 5) < 0);

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 0);
        sp.addForwardedField(0, 0, 4);
        sp.addForwardedField(0, 1, 1);
        sp.addForwardedField(0, 1, 2);
        sp.addForwardedField(0, 1, 3);

        Assertions.assertEquals(0, sp.getForwardingSourceField(0, 0));
        Assertions.assertEquals(0, sp.getForwardingSourceField(0, 4));
        Assertions.assertEquals(1, sp.getForwardingSourceField(0, 1));
        Assertions.assertEquals(1, sp.getForwardingSourceField(0, 2));
        Assertions.assertEquals(1, sp.getForwardingSourceField(0, 3));
        Assertions.assertTrue(sp.getForwardingSourceField(0, 5) < 0);

        // second input
        sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 1);
        sp.addForwardedField(1, 1, 4);
        sp.addForwardedField(1, 2, 3);
        sp.addForwardedField(1, 3, 2);

        Assertions.assertEquals(0, sp.getForwardingSourceField(1, 1));
        Assertions.assertEquals(1, sp.getForwardingSourceField(1, 4));
        Assertions.assertEquals(2, sp.getForwardingSourceField(1, 3));
        Assertions.assertEquals(3, sp.getForwardingSourceField(1, 2));
        Assertions.assertTrue(sp.getForwardingSourceField(1, 0) < 0);
        Assertions.assertTrue(sp.getForwardingSourceField(1, 5) < 0);

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 0);
        sp.addForwardedField(1, 0, 4);
        sp.addForwardedField(1, 1, 1);
        sp.addForwardedField(1, 1, 2);
        sp.addForwardedField(1, 1, 3);

        Assertions.assertEquals(0, sp.getForwardingSourceField(1, 0));
        Assertions.assertEquals(0, sp.getForwardingSourceField(1, 4));
        Assertions.assertEquals(1, sp.getForwardingSourceField(1, 1));
        Assertions.assertEquals(1, sp.getForwardingSourceField(1, 2));
        Assertions.assertEquals(1, sp.getForwardingSourceField(1, 3));
        Assertions.assertTrue(sp.getForwardingSourceField(1, 5) < 0);
    }

    @Test
    void testGetReadSet() {

        // first input
        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addReadFields(0, new FieldSet(0, 1));

        Assertions.assertEquals(2, sp.getReadFields(0).size());
        Assertions.assertTrue(sp.getReadFields(0).contains(0));
        Assertions.assertTrue(sp.getReadFields(0).contains(1));

        sp.addReadFields(0, new FieldSet(3));

        Assertions.assertEquals(3, sp.getReadFields(0).size());
        Assertions.assertTrue(sp.getReadFields(0).contains(0));
        Assertions.assertTrue(sp.getReadFields(0).contains(1));
        Assertions.assertTrue(sp.getReadFields(0).contains(3));

        // second input
        sp = new DualInputSemanticProperties();
        sp.addReadFields(1, new FieldSet(0, 1));

        Assertions.assertEquals(2, sp.getReadFields(1).size());
        Assertions.assertTrue(sp.getReadFields(1).contains(0));
        Assertions.assertTrue(sp.getReadFields(1).contains(1));

        sp.addReadFields(1, new FieldSet(3));

        Assertions.assertEquals(3, sp.getReadFields(1).size());
        Assertions.assertTrue(sp.getReadFields(1).contains(0));
        Assertions.assertTrue(sp.getReadFields(1).contains(1));
        Assertions.assertTrue(sp.getReadFields(1).contains(3));
    }

    @Test(expected = SemanticProperties.InvalidSemanticAnnotationException.class)
    public void testAddForwardedFieldsTargetTwice1() {

        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 2);
        sp.addForwardedField(0, 1, 2);
    }

    @Test(expected = SemanticProperties.InvalidSemanticAnnotationException.class)
    public void testAddForwardedFieldsTargetTwice2() {

        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 2);
        sp.addForwardedField(1, 1, 2);
    }
}

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

package org.apache.flink.api.common.typeutils;

import org.assertj.core.api.Condition;

import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Collection of useful {@link Condition}s for {@link TypeSerializer} and {@link
 * TypeSerializerSchemaCompatibility}.
 */
public final class TypeSerializerMatchers {

    private TypeSerializerMatchers() {}

    // -------------------------------------------------------------------------------------------------------------
    // Condition Factories
    // -------------------------------------------------------------------------------------------------------------

    /**
     * Matches {@code compatibleAsIs} {@link TypeSerializerSchemaCompatibility}.
     *
     * @param <T> element type
     * @return a {@code Condition} that matches {@code compatibleAsIs} {@link
     *     TypeSerializerSchemaCompatibility}.
     */
    public static <T> Condition<TypeSerializerSchemaCompatibility<T>> isCompatibleAsIs() {
        return propertyMatcher(
                TypeSerializerSchemaCompatibility::isCompatibleAsIs,
                "type serializer schema that is a compatible as is");
    }

    /**
     * Matches {@code isIncompatible} {@link TypeSerializerSchemaCompatibility}.
     *
     * @param <T> element type
     * @return a {@code Condition} that matches {@code isIncompatible} {@link
     *     TypeSerializerSchemaCompatibility}.
     */
    public static <T> Condition<TypeSerializerSchemaCompatibility<T>> isIncompatible() {
        return propertyMatcher(
                TypeSerializerSchemaCompatibility::isIncompatible,
                "type serializer schema that is incompatible");
    }

    /**
     * Matches {@code isCompatibleAfterMigration} {@link TypeSerializerSchemaCompatibility}.
     *
     * @param <T> element type
     * @return a {@code Condition} that matches {@code isCompatibleAfterMigration} {@link
     *     TypeSerializerSchemaCompatibility}.
     */
    public static <T> Condition<TypeSerializerSchemaCompatibility<T>> isCompatibleAfterMigration() {
        return propertyMatcher(
                TypeSerializerSchemaCompatibility::isCompatibleAfterMigration,
                "type serializer schema that is compatible after migration");
    }

    /**
     * Matches {@code isCompatibleWithReconfiguredSerializer} {@link
     * TypeSerializerSchemaCompatibility}.
     *
     * @param <T> element type
     * @return a {@code Condition} that matches {@code isCompatibleWithReconfiguredSerializer}
     *     {@link TypeSerializerSchemaCompatibility}.
     */
    public static <T>
            Condition<TypeSerializerSchemaCompatibility<T>>
                    isCompatibleWithReconfiguredSerializer() {
        @SuppressWarnings("unchecked")
        Condition<TypeSerializer<T>> anything = new Condition<>(t -> true, "anything");

        return new CompatibleAfterReconfiguration<>(anything);
    }

    /**
     * Matches {@code isCompatibleWithReconfiguredSerializer} {@link
     * TypeSerializerSchemaCompatibility}.
     *
     * @param reconfiguredSerializerMatcher matches the reconfigured serializer.
     * @param <T> element type
     * @return a {@code Condition} that matches {@code isCompatibleWithReconfiguredSerializer}
     *     {@link TypeSerializerSchemaCompatibility}.
     */
    public static <T>
            Condition<TypeSerializerSchemaCompatibility<T>> isCompatibleWithReconfiguredSerializer(
                    Condition<TypeSerializer<T>> reconfiguredSerializerMatcher) {

        return new CompatibleAfterReconfiguration<>(reconfiguredSerializerMatcher);
    }

    /**
     * Matches if the expected {@code TypeSerializerSchemaCompatibility} has the same compatibility
     * as {@code expectedCompatibility}.
     *
     * @param expectedCompatibility the compatibility to match to.
     * @param <T> element type.
     * @return a {@code Condition} that matches if it has the same compatibility as {@code
     *     expectedCompatibility}.
     */
    public static <T> Condition<TypeSerializerSchemaCompatibility<T>> hasSameCompatibilityAs(
            TypeSerializerSchemaCompatibility<T> expectedCompatibility) {

        return new SchemaCompatibilitySameAs<>(expectedCompatibility);
    }

    // -------------------------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------------------------

    private static <T> Condition<T> propertyMatcher(
            Predicate<T> predicate, String matcherDescription) {
        return new Condition<T>(predicate, matcherDescription);
    }

    // -------------------------------------------------------------------------------------------------------------
    // Matchers
    // -------------------------------------------------------------------------------------------------------------

    private static final class CompatibleAfterReconfiguration<T>
            extends Condition<TypeSerializerSchemaCompatibility<T>> {

        private final Condition<TypeSerializer<T>> reconfiguredSerializerMatcher;

        private CompatibleAfterReconfiguration(
                Condition<TypeSerializer<T>> reconfiguredSerializerMatcher) {
            this.reconfiguredSerializerMatcher = checkNotNull(reconfiguredSerializerMatcher);
        }

        @Override
        public boolean matches(TypeSerializerSchemaCompatibility<T> item) {
            if (!item.isCompatibleWithReconfiguredSerializer()) {

                return false;
            }
            TypeSerializer<T> reconfiguredSerializer = item.getReconfiguredSerializer();
            return reconfiguredSerializerMatcher.matches(reconfiguredSerializer);
        }
    }

    private static class SchemaCompatibilitySameAs<T>
            extends Condition<TypeSerializerSchemaCompatibility<T>> {

        private final TypeSerializerSchemaCompatibility<T> expectedCompatibility;

        private SchemaCompatibilitySameAs(
                TypeSerializerSchemaCompatibility<T> expectedCompatibility) {
            this.expectedCompatibility = checkNotNull(expectedCompatibility);
        }

        @Override
        public boolean matches(TypeSerializerSchemaCompatibility<T> testResultCompatibility) {
            if (expectedCompatibility.isCompatibleAsIs()) {
                return testResultCompatibility.isCompatibleAsIs();
            } else if (expectedCompatibility.isIncompatible()) {
                return testResultCompatibility.isIncompatible();
            } else if (expectedCompatibility.isCompatibleAfterMigration()) {
                return testResultCompatibility.isCompatibleAfterMigration();
            } else if (expectedCompatibility.isCompatibleWithReconfiguredSerializer()) {
                return testResultCompatibility.isCompatibleWithReconfiguredSerializer();
            }
            return false;
        }
    }
}

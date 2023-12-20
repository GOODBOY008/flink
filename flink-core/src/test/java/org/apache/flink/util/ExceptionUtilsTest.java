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

package org.apache.flink.util;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/** Tests for the utility methods in {@link ExceptionUtils}. */
public class ExceptionUtilsTest extends TestLogger {

    @Test
    void testStringifyNullException() {
        Assertions.assertNotNull(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION);
        Assertions.assertEquals(
                ExceptionUtils.STRINGIFIED_NULL_EXCEPTION, ExceptionUtils.stringifyException(null));
    }

    @Test
    void testJvmFatalError() {
        // not all errors are fatal
        Assertions.assertFalse(ExceptionUtils.isJvmFatalError(new Error()));

        // linkage errors are not fatal
        Assertions.assertFalse(ExceptionUtils.isJvmFatalError(new LinkageError()));

        // some errors are fatal
        Assertions.assertTrue(ExceptionUtils.isJvmFatalError(new InternalError()));
        Assertions.assertTrue(ExceptionUtils.isJvmFatalError(new UnknownError()));
    }

    @Test
    void testRethrowFatalError() {
        // fatal error is rethrown
        try {
            ExceptionUtils.rethrowIfFatalError(new InternalError());
            Assertions.fail();
        } catch (InternalError ignored) {
        }

        // non-fatal error is not rethrown
        ExceptionUtils.rethrowIfFatalError(new NoClassDefFoundError());
    }

    @Test
    void testFindThrowableByType() {
        Assertions.assertTrue(
                ExceptionUtils.findThrowable(
                                new RuntimeException(new IllegalStateException()),
                                IllegalStateException.class)
                        .isPresent());
    }

    @Test
    void testExceptionStripping() {
        final FlinkException expectedException = new FlinkException("test exception");
        final Throwable strippedException =
                ExceptionUtils.stripException(
                        new RuntimeException(new RuntimeException(expectedException)),
                        RuntimeException.class);

        MatcherAssert.assertThat(strippedException, is(equalTo(expectedException)));
    }

    @Test
    void testInvalidExceptionStripping() {
        final FlinkException expectedException =
                new FlinkException(new RuntimeException(new FlinkException("inner exception")));
        final Throwable strippedException =
                ExceptionUtils.stripException(expectedException, RuntimeException.class);

        MatcherAssert.assertThat(strippedException, is(equalTo(expectedException)));
    }

    @Test
    void testTryEnrichTaskExecutorErrorCanHandleNullValueWithoutCausingException() {
        ExceptionUtils.tryEnrichOutOfMemoryError(null, "", "", "");
    }

    @Test
    void testUpdateDetailMessageOfBasicThrowable() {
        Throwable rootThrowable = new OutOfMemoryError("old message");
        ExceptionUtils.updateDetailMessage(rootThrowable, t -> "new message");

        MatcherAssert.assertThat(rootThrowable.getMessage(), is("new message"));
    }

    @Test
    void testUpdateDetailMessageOfRelevantThrowableAsCause() {
        Throwable oomCause =
                new IllegalArgumentException("another message deep down in the cause tree");

        Throwable oom = new OutOfMemoryError("old message").initCause(oomCause);
        oom.setStackTrace(
                new StackTraceElement[] {new StackTraceElement("class", "method", "file", 1)});
        oom.addSuppressed(new NullPointerException());

        Throwable rootThrowable = new IllegalStateException("another message", oom);
        ExceptionUtils.updateDetailMessage(
                rootThrowable,
                t -> t.getClass().equals(OutOfMemoryError.class) ? "new message" : null);

        MatcherAssert.assertThat(rootThrowable.getCause(), sameInstance(oom));
        MatcherAssert.assertThat(rootThrowable.getCause().getMessage(), is("new message"));
        MatcherAssert.assertThat(rootThrowable.getCause().getStackTrace(), is(oom.getStackTrace()));
        MatcherAssert.assertThat(rootThrowable.getCause().getSuppressed(), is(oom.getSuppressed()));

        MatcherAssert.assertThat(rootThrowable.getCause().getCause(), sameInstance(oomCause));
    }

    @Test
    void testUpdateDetailMessageWithoutRelevantThrowable() {
        Throwable originalThrowable =
                new IllegalStateException(
                        "root message", new IllegalArgumentException("cause message"));
        ExceptionUtils.updateDetailMessage(originalThrowable, t -> null);

        MatcherAssert.assertThat(originalThrowable.getMessage(), is("root message"));
        MatcherAssert.assertThat(originalThrowable.getCause().getMessage(), is("cause message"));
    }

    @Test
    void testUpdateDetailMessageOfNullWithoutException() {
        ExceptionUtils.updateDetailMessage(null, t -> "new message");
    }

    @Test
    void testUpdateDetailMessageWithMissingPredicate() {
        Throwable root = new Exception("old message");
        ExceptionUtils.updateDetailMessage(root, null);

        MatcherAssert.assertThat(root.getMessage(), is("old message"));
    }

    @Test
    void testIsMetaspaceOutOfMemoryErrorCanHandleNullValue() {
        Assertions.assertFalse(ExceptionUtils.isMetaspaceOutOfMemoryError(null));
    }

    @Test
    void testIsDirectOutOfMemoryErrorCanHandleNullValue() {
        Assertions.assertFalse(ExceptionUtils.isDirectOutOfMemoryError(null));
    }
}

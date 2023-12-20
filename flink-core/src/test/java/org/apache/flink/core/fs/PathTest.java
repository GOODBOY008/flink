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

package org.apache.flink.core.fs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;

/** Tests for the {@link Path} class. */
public class PathTest {

    @Test
    void testPathFromString() {

        Path p = new Path("/my/path");
        Assertions.assertEquals("/my/path", p.toUri().getPath());
        Assertions.assertNull(p.toUri().getScheme());

        p = new Path("/my/path/");
        Assertions.assertEquals("/my/path", p.toUri().getPath());
        Assertions.assertNull(p.toUri().getScheme());

        p = new Path("/my//path/");
        Assertions.assertEquals("/my/path", p.toUri().getPath());
        Assertions.assertNull(p.toUri().getScheme());

        p = new Path("/my//path//a///");
        Assertions.assertEquals("/my/path/a", p.toUri().getPath());
        Assertions.assertNull(p.toUri().getScheme());

        p = new Path("\\my\\path\\\\a\\\\\\");
        Assertions.assertEquals("/my/path/a", p.toUri().getPath());
        Assertions.assertNull(p.toUri().getScheme());

        p = new Path("hdfs:///my/path");
        Assertions.assertEquals("/my/path", p.toUri().getPath());
        Assertions.assertEquals("hdfs", p.toUri().getScheme());

        p = new Path("hdfs:///my/path/");
        Assertions.assertEquals("/my/path", p.toUri().getPath());
        Assertions.assertEquals("hdfs", p.toUri().getScheme());

        p = new Path("file:///my/path");
        Assertions.assertEquals("/my/path", p.toUri().getPath());
        Assertions.assertEquals("file", p.toUri().getScheme());

        p = new Path("C:/my/windows/path");
        Assertions.assertEquals("/C:/my/windows/path", p.toUri().getPath());

        p = new Path("file:/C:/my/windows/path");
        Assertions.assertEquals("/C:/my/windows/path", p.toUri().getPath());

        try {
            new Path((String) null);
            Assertions.fail();
        } catch (Exception e) {
            // exception expected
        }

        try {
            new Path("");
            Assertions.fail();
        } catch (Exception e) {
            // exception expected
        }
    }

    @Test
    void testIsAbsolute() {

        // UNIX

        Path p = new Path("/my/abs/path");
        Assertions.assertTrue(p.isAbsolute());

        p = new Path("/");
        Assertions.assertTrue(p.isAbsolute());

        p = new Path("./my/rel/path");
        Assertions.assertFalse(p.isAbsolute());

        p = new Path("my/rel/path");
        Assertions.assertFalse(p.isAbsolute());

        // WINDOWS

        p = new Path("C:/my/abs/windows/path");
        Assertions.assertTrue(p.isAbsolute());

        p = new Path("y:/my/abs/windows/path");
        Assertions.assertTrue(p.isAbsolute());

        p = new Path("/y:/my/abs/windows/path");
        Assertions.assertTrue(p.isAbsolute());

        p = new Path("b:\\my\\abs\\windows\\path");
        Assertions.assertTrue(p.isAbsolute());

        p = new Path("/c:/my/dir");
        Assertions.assertTrue(p.isAbsolute());

        p = new Path("/C:/");
        Assertions.assertTrue(p.isAbsolute());

        p = new Path("C:");
        Assertions.assertFalse(p.isAbsolute());

        p = new Path("C:/");
        Assertions.assertTrue(p.isAbsolute());

        p = new Path("C:my\\relative\\path");
        Assertions.assertFalse(p.isAbsolute());

        p = new Path("\\my\\dir");
        Assertions.assertTrue(p.isAbsolute());

        p = new Path("\\");
        Assertions.assertTrue(p.isAbsolute());

        p = new Path(".\\my\\relative\\path");
        Assertions.assertFalse(p.isAbsolute());

        p = new Path("my\\relative\\path");
        Assertions.assertFalse(p.isAbsolute());

        p = new Path("\\\\myServer\\myDir");
        Assertions.assertTrue(p.isAbsolute());
    }

    @Test
    void testGetName() {

        Path p = new Path("/my/fancy/path");
        Assertions.assertEquals("path", p.getName());

        p = new Path("/my/fancy/path/");
        Assertions.assertEquals("path", p.getName());

        p = new Path("hdfs:///my/path");
        Assertions.assertEquals("path", p.getName());

        p = new Path("hdfs:///myPath/");
        Assertions.assertEquals("myPath", p.getName());

        p = new Path("/");
        Assertions.assertEquals("", p.getName());

        p = new Path("C:/my/windows/path");
        Assertions.assertEquals("path", p.getName());

        p = new Path("file:/C:/my/windows/path");
        Assertions.assertEquals("path", p.getName());
    }

    @Test
    void testGetParent() {

        Path p = new Path("/my/fancy/path");
        Assertions.assertEquals("/my/fancy", p.getParent().toUri().getPath());

        p = new Path("/my/other/fancy/path/");
        Assertions.assertEquals("/my/other/fancy", p.getParent().toUri().getPath());

        p = new Path("hdfs:///my/path");
        Assertions.assertEquals("/my", p.getParent().toUri().getPath());

        p = new Path("hdfs:///myPath/");
        Assertions.assertEquals("/", p.getParent().toUri().getPath());

        p = new Path("/");
        Assertions.assertNull(p.getParent());

        p = new Path("C:/my/windows/path");
        Assertions.assertEquals("/C:/my/windows", p.getParent().toUri().getPath());
    }

    @Test
    void testSuffix() {

        Path p = new Path("/my/path");
        p = p.suffix("_123");
        Assertions.assertEquals("/my/path_123", p.toUri().getPath());

        p = new Path("/my/path/");
        p = p.suffix("/abc");
        Assertions.assertEquals("/my/path/abc", p.toUri().getPath());

        p = new Path("C:/my/windows/path");
        p = p.suffix("/abc");
        Assertions.assertEquals("/C:/my/windows/path/abc", p.toUri().getPath());
    }

    @Test
    void testDepth() {

        Path p = new Path("/my/path");
        Assertions.assertEquals(2, p.depth());

        p = new Path("/my/fancy/path/");
        Assertions.assertEquals(3, p.depth());

        p = new Path("/my/fancy/fancy/fancy/fancy/fancy/fancy/fancy/fancy/fancy/fancy/path");
        Assertions.assertEquals(12, p.depth());

        p = new Path("/");
        Assertions.assertEquals(0, p.depth());

        p = new Path("C:/my/windows/path");
        Assertions.assertEquals(4, p.depth());
    }

    @Test
    void testParsing() {
        URI u;
        String scheme = "hdfs";
        String authority = "localhost:8000";
        String path = "/test/test";

        // correct usage
        // hdfs://localhost:8000/test/test
        u = new Path(scheme + "://" + authority + path).toUri();
        Assertions.assertEquals(scheme, u.getScheme());
        Assertions.assertEquals(authority, u.getAuthority());
        Assertions.assertEquals(path, u.getPath());
        // hdfs:///test/test
        u = new Path(scheme + "://" + path).toUri();
        Assertions.assertEquals(scheme, u.getScheme());
        Assertions.assertNull(u.getAuthority());
        Assertions.assertEquals(path, u.getPath());
        // hdfs:/test/test
        u = new Path(scheme + ":" + path).toUri();
        Assertions.assertEquals(scheme, u.getScheme());
        Assertions.assertNull(u.getAuthority());
        Assertions.assertEquals(path, u.getPath());

        // incorrect usage
        // hdfs://test/test
        u = new Path(scheme + ":/" + path).toUri();
        Assertions.assertEquals(scheme, u.getScheme());
        Assertions.assertEquals("test", u.getAuthority());
        Assertions.assertEquals("/test", u.getPath());
        // hdfs:////test/test
        u = new Path(scheme + ":///" + path).toUri();
        Assertions.assertEquals("hdfs", u.getScheme());
        Assertions.assertNull(u.getAuthority());
        Assertions.assertEquals(path, u.getPath());
    }

    @Test
    void testMakeQualified() {
        // make relative path qualified
        String path = "test/test";
        Path p = new Path(path).makeQualified(FileSystem.getLocalFileSystem());
        URI u = p.toUri();

        Assertions.assertEquals("file", u.getScheme());
        Assertions.assertNull(u.getAuthority());

        String q =
                new Path(FileSystem.getLocalFileSystem().getWorkingDirectory().getPath(), path)
                        .getPath();
        Assertions.assertEquals(q, u.getPath());

        // make absolute path qualified
        path = "/test/test";
        p = new Path(path).makeQualified(FileSystem.getLocalFileSystem());
        u = p.toUri();
        Assertions.assertEquals("file", u.getScheme());
        Assertions.assertNull(u.getAuthority());
        Assertions.assertEquals(path, u.getPath());
    }
}

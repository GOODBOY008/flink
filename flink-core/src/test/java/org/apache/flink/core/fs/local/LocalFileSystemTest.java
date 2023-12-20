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

package org.apache.flink.core.fs.local;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.testutils.junit.FailsInGHAContainerWithRootUser;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

/**
 * This class tests the functionality of the {@link LocalFileSystem} class in its components. In
 * particular, file/directory access, creation, deletion, read, write is tested.
 */
public class LocalFileSystemTest extends TestLogger {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    /** This test checks the functionality of the {@link LocalFileSystem} class. */
    @Test
    void testLocalFilesystem() throws Exception {
        final File tempdir = new File(temporaryFolder.getRoot(), UUID.randomUUID().toString());

        final File testfile1 = new File(tempdir, UUID.randomUUID().toString());
        final File testfile2 = new File(tempdir, UUID.randomUUID().toString());

        final Path pathtotestfile1 = new Path(testfile1.toURI().getPath());
        final Path pathtotestfile2 = new Path(testfile2.toURI().getPath());

        final LocalFileSystem lfs = new LocalFileSystem();

        final Path pathtotmpdir = new Path(tempdir.toURI().getPath());

        /*
         * check that lfs can see/create/delete/read directories
         */

        // check that dir is not existent yet
        Assertions.assertFalse(lfs.exists(pathtotmpdir));
        Assertions.assertTrue(tempdir.mkdirs());

        // check that local file system recognizes file..
        Assertions.assertTrue(lfs.exists(pathtotmpdir));
        final FileStatus localstatus1 = lfs.getFileStatus(pathtotmpdir);

        // check that lfs recognizes directory..
        Assertions.assertTrue(localstatus1.isDir());

        // get status for files in this (empty) directory..
        final FileStatus[] statusforfiles = lfs.listStatus(pathtotmpdir);

        // no files in there.. hence, must be zero
        Assertions.assertEquals(0, statusforfiles.length);

        // check that lfs can delete directory..
        lfs.delete(pathtotmpdir, true);

        // double check that directory is not existent anymore..
        Assertions.assertFalse(lfs.exists(pathtotmpdir));
        Assertions.assertFalse(tempdir.exists());

        // re-create directory..
        lfs.mkdirs(pathtotmpdir);

        // creation successful?
        Assertions.assertTrue(tempdir.exists());

        /*
         * check that lfs can create/read/write from/to files properly and read meta information..
         */

        // create files.. one ""natively"", one using lfs
        final FSDataOutputStream lfsoutput1 = lfs.create(pathtotestfile1, WriteMode.NO_OVERWRITE);
        Assertions.assertTrue(testfile2.createNewFile());

        // does lfs create files? does lfs recognize created files?
        Assertions.assertTrue(testfile1.exists());
        Assertions.assertTrue(lfs.exists(pathtotestfile2));

        // test that lfs can write to files properly
        final byte[] testbytes = {1, 2, 3, 4, 5};
        lfsoutput1.write(testbytes);
        lfsoutput1.close();

        Assertions.assertEquals(testfile1.length(), 5L);

        byte[] testbytestest = new byte[5];
        try (FileInputStream fisfile1 = new FileInputStream(testfile1)) {
            Assertions.assertEquals(testbytestest.length, fisfile1.read(testbytestest));
        }

        Assertions.assertArrayEquals(testbytes, testbytestest);

        // does lfs see the correct file length?
        Assertions.assertEquals(lfs.getFileStatus(pathtotestfile1).getLen(), testfile1.length());

        // as well, when we call the listStatus (that is intended for directories?)
        Assertions.assertEquals(lfs.listStatus(pathtotestfile1)[0].getLen(), testfile1.length());

        // test that lfs can read files properly
        final FileOutputStream fosfile2 = new FileOutputStream(testfile2);
        fosfile2.write(testbytes);
        fosfile2.close();

        testbytestest = new byte[5];
        final FSDataInputStream lfsinput2 = lfs.open(pathtotestfile2);
        Assertions.assertEquals(lfsinput2.read(testbytestest), 5);
        lfsinput2.close();
        Assertions.assertArrayEquals(testbytes, testbytestest);

        // does lfs see two files?
        Assertions.assertEquals(lfs.listStatus(pathtotmpdir).length, 2);

        // do we get exactly one blocklocation per file? no matter what start and len we provide
        Assertions.assertEquals(
                lfs.getFileBlockLocations(lfs.getFileStatus(pathtotestfile1), 0, 0).length, 1);

        /*
         * can lfs delete files / directories?
         */
        Assertions.assertTrue(lfs.delete(pathtotestfile1, false));

        // and can lfs also delete directories recursively?
        Assertions.assertTrue(lfs.delete(pathtotmpdir, true));

        Assertions.assertFalse(tempdir.exists());
    }

    @Test
    void testRenamePath() throws IOException {
        final File rootDirectory = temporaryFolder.newFolder();

        // create a file /root/src/B/test.csv
        final File srcDirectory = new File(new File(rootDirectory, "src"), "B");
        Assertions.assertTrue(srcDirectory.mkdirs());

        final File srcFile = new File(srcDirectory, "test.csv");
        Assertions.assertTrue(srcFile.createNewFile());

        // Move/rename B and its content to /root/dst/A
        final File destDirectory = new File(new File(rootDirectory, "dst"), "B");
        final File destFile = new File(destDirectory, "test.csv");

        final Path srcDirPath = new Path(srcDirectory.toURI());
        final Path srcFilePath = new Path(srcFile.toURI());
        final Path destDirPath = new Path(destDirectory.toURI());
        final Path destFilePath = new Path(destFile.toURI());

        FileSystem fs = FileSystem.getLocalFileSystem();

        // pre-conditions: /root/src/B exists but /root/dst/B does not
        Assertions.assertTrue(fs.exists(srcDirPath));
        Assertions.assertFalse(fs.exists(destDirPath));

        // do the move/rename: /root/src/B -> /root/dst/
        Assertions.assertTrue(fs.rename(srcDirPath, destDirPath));

        // post-conditions: /root/src/B doesn't exists, /root/dst/B/test.csv has been created
        Assertions.assertTrue(fs.exists(destFilePath));
        Assertions.assertFalse(fs.exists(srcDirPath));

        // re-create source file and test overwrite
        Assertions.assertTrue(srcDirectory.mkdirs());
        Assertions.assertTrue(srcFile.createNewFile());

        // overwrite the destination file
        Assertions.assertTrue(fs.rename(srcFilePath, destFilePath));

        // post-conditions: now only the src file has been moved
        Assertions.assertFalse(fs.exists(srcFilePath));
        Assertions.assertTrue(fs.exists(srcDirPath));
        Assertions.assertTrue(fs.exists(destFilePath));
    }

    @Test
    void testRenameNonExistingFile() throws IOException {
        final FileSystem fs = FileSystem.getLocalFileSystem();

        final File srcFile = new File(temporaryFolder.newFolder(), "someFile.txt");
        final File destFile = new File(temporaryFolder.newFolder(), "target");

        final Path srcFilePath = new Path(srcFile.toURI());
        final Path destFilePath = new Path(destFile.toURI());

        // this cannot succeed because the source file does not exist
        Assertions.assertFalse(fs.rename(srcFilePath, destFilePath));
    }

    @Test
    @Category(FailsInGHAContainerWithRootUser.class)
    void testRenameFileWithNoAccess() throws IOException {
        final FileSystem fs = FileSystem.getLocalFileSystem();

        final File srcFile = temporaryFolder.newFile("someFile.txt");
        final File destFile = new File(temporaryFolder.newFolder(), "target");

        // we need to make the file non-modifiable so that the rename fails
        Assumptions.assumeTrue(srcFile.getParentFile().setWritable(false, false));
        Assumptions.assumeTrue(srcFile.setWritable(false, false));

        try {
            final Path srcFilePath = new Path(srcFile.toURI());
            final Path destFilePath = new Path(destFile.toURI());

            // this cannot succeed because the source folder has no permission to remove the file
            Assertions.assertFalse(fs.rename(srcFilePath, destFilePath));
        } finally {
            // make sure we give permission back to ensure proper cleanup

            //noinspection ResultOfMethodCallIgnored
            srcFile.getParentFile().setWritable(true, false);
            //noinspection ResultOfMethodCallIgnored
            srcFile.setWritable(true, false);
        }
    }

    @Test
    void testRenameToNonEmptyTargetDir() throws IOException {
        final FileSystem fs = FileSystem.getLocalFileSystem();

        // a source folder with a file
        final File srcFolder = temporaryFolder.newFolder();
        final File srcFile = new File(srcFolder, "someFile.txt");
        Assertions.assertTrue(srcFile.createNewFile());

        // a non-empty destination folder
        final File dstFolder = temporaryFolder.newFolder();
        final File dstFile = new File(dstFolder, "target");
        Assertions.assertTrue(dstFile.createNewFile());

        // this cannot succeed because the destination folder is not empty
        Assertions.assertFalse(fs.rename(new Path(srcFolder.toURI()), new Path(dstFolder.toURI())));

        // retry after deleting the occupying target file
        Assertions.assertTrue(dstFile.delete());
        Assertions.assertTrue(fs.rename(new Path(srcFolder.toURI()), new Path(dstFolder.toURI())));
        Assertions.assertTrue(new File(dstFolder, srcFile.getName()).exists());
    }

    @Test
    void testKind() {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        Assertions.assertEquals(FileSystemKind.FILE_SYSTEM, fs.getKind());
    }

    @Test
    void testConcurrentMkdirs() throws Exception {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        final File root = temporaryFolder.getRoot();
        final int directoryDepth = 10;
        final int concurrentOperations = 10;

        final Collection<File> targetDirectories =
                createTargetDirectories(root, directoryDepth, concurrentOperations);

        final ExecutorService executor = Executors.newFixedThreadPool(concurrentOperations);
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(concurrentOperations);

        try {
            final Collection<CompletableFuture<Void>> mkdirsFutures =
                    new ArrayList<>(concurrentOperations);
            for (File targetDirectory : targetDirectories) {
                final CompletableFuture<Void> mkdirsFuture =
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        cyclicBarrier.await();
                                        MatcherAssert.assertThat(
                                                fs.mkdirs(Path.fromLocalFile(targetDirectory)),
                                                is(true));
                                    } catch (Exception e) {
                                        throw new CompletionException(e);
                                    }
                                },
                                executor);

                mkdirsFutures.add(mkdirsFuture);
            }

            final CompletableFuture<Void> allFutures =
                    CompletableFuture.allOf(
                            mkdirsFutures.toArray(new CompletableFuture[concurrentOperations]));

            allFutures.get();
        } finally {
            final long timeout = 10000L;
            ExecutorUtils.gracefulShutdown(timeout, TimeUnit.MILLISECONDS, executor);
        }
    }

    /** This test verifies the issue https://issues.apache.org/jira/browse/FLINK-18612. */
    @Test
    void testCreatingFileInCurrentDirectoryWithRelativePath() throws IOException {
        FileSystem fs = FileSystem.getLocalFileSystem();

        Path filePath = new Path("local_fs_test_" + RandomStringUtils.randomAlphanumeric(16));
        try (FSDataOutputStream outputStream = fs.create(filePath, WriteMode.OVERWRITE)) {
            // Do nothing.
        } finally {
            for (int i = 0; i < 10 && fs.exists(filePath); ++i) {
                fs.delete(filePath, true);
            }
        }
    }

    @Test(expected = ClosedChannelException.class)
    public void testFlushMethodFailsOnClosedOutputStream() throws IOException {
        testMethodCallFailureOnClosedStream(FSDataOutputStream::flush);
    }

    @Test(expected = ClosedChannelException.class)
    public void testWriteIntegerMethodFailsOnClosedOutputStream() throws IOException {
        testMethodCallFailureOnClosedStream(os -> os.write(0));
    }

    @Test(expected = ClosedChannelException.class)
    public void testWriteBytesMethodFailsOnClosedOutputStream() throws IOException {
        testMethodCallFailureOnClosedStream(os -> os.write(new byte[0]));
    }

    @Test(expected = ClosedChannelException.class)
    public void testWriteBytesSubArrayMethodFailsOnClosedOutputStream() throws IOException {
        testMethodCallFailureOnClosedStream(os -> os.write(new byte[0], 0, 0));
    }

    @Test(expected = ClosedChannelException.class)
    public void testGetPosMethodFailsOnClosedOutputStream() throws IOException {
        testMethodCallFailureOnClosedStream(FSDataOutputStream::getPos);
    }

    private void testMethodCallFailureOnClosedStream(
            ThrowingConsumer<FSDataOutputStream, IOException> callback) throws IOException {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        final FSDataOutputStream outputStream =
                fs.create(
                        new Path(
                                temporaryFolder.getRoot().toString(),
                                "close_fs_test_" + UUID.randomUUID()),
                        WriteMode.OVERWRITE);
        outputStream.close();
        callback.accept(outputStream);
    }

    private Collection<File> createTargetDirectories(
            File root, int directoryDepth, int numberDirectories) {
        final StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < directoryDepth; i++) {
            stringBuilder.append('/').append(i);
        }

        final Collection<File> targetDirectories = new ArrayList<>(numberDirectories);

        for (int i = 0; i < numberDirectories; i++) {
            targetDirectories.add(new File(root, stringBuilder.toString() + '/' + i));
        }

        return targetDirectories;
    }
}

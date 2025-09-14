import org.junit.jupiter.api.*;
import shared.FileHandler;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.*;

class FileHandlerTest {

    private static final String TEST_STATION_ID = "station123";
    private static final Path MAIN_FILE = Paths.get(TEST_STATION_ID + ".json");
    private static final Path TEMP_FILE = Paths.get(TEST_STATION_ID + "-temp.json");

    @BeforeEach
    void cleanUpBefore() throws IOException {
        Files.deleteIfExists(MAIN_FILE);
        Files.deleteIfExists(TEMP_FILE);
    }

    @AfterEach
    void cleanUpAfter() throws IOException {
        Files.deleteIfExists(MAIN_FILE);
        Files.deleteIfExists(TEMP_FILE);
    }

    @Test
    void testWriteToFile_WritesSuccessfully() throws IOException {
        FileHandler handler = new FileHandler(TEST_STATION_ID);
        handler.writeToFile("{\"msg\":\"hello\"}", 5, 0,0,"",0);

        assertTrue(Files.exists(MAIN_FILE), "Main file should exist after write");

        // Read back contents
        String content = Files.readString(MAIN_FILE);
        assertTrue(content.contains("\"lamport\": 5"));
        assertTrue(content.contains("\"msg\":\"hello\""));
    }

    @Test
    void testWriteToFile_NewerLamport_WritesSuccessfully() throws IOException {
        FileHandler handler = new FileHandler(TEST_STATION_ID);
        handler.writeToFile("{\"msg\":\"hello\"}", 5, 0,0,"",0);
        handler.writeToFile("{\"msg\":\"hello2\"}", 10, 0,0,"",0);

        // Read back contents
        String content = Files.readString(MAIN_FILE);
        assertTrue(content.contains("\"lamport\": 10"));
        assertTrue(content.contains("\"msg\":\"hello2\""));
    }

    @Test
    void testWriteToFile_OlderLamportValue_Ignored() throws IOException {
        FileHandler handler = new FileHandler(TEST_STATION_ID);
        handler.writeToFile("{\"msg\":\"new\"}", 10, 0,0,"",0);

        // Attempt with older lamport
        handler.writeToFile("{\"msg\":\"old\"}", 5, 0,0,"",0);

        String content = Files.readString(MAIN_FILE);
        assertTrue(content.contains("\"msg\":\"new\""), "Older lamport should not overwrite");
        assertFalse(content.contains("\"msg\":\"old\""));
    }

    @Test
    void testWriteToFile_SameLamportValue_Ignored() throws IOException {
        FileHandler handler = new FileHandler(TEST_STATION_ID);
        handler.writeToFile("{\"msg\":\"new\"}", 10, 0,0,"",0);

        // Attempt with same lamport
        handler.writeToFile("{\"msg\":\"old\"}", 10, 0,0,"",0);

        String content = Files.readString(MAIN_FILE);
        assertTrue(content.contains("\"msg\":\"new\""), "Same lamport should not overwrite");
        assertFalse(content.contains("\"msg\":\"old\""));
    }

    @Test
    void testSafeWrite_FileMoveFailure_ShowsError() {
        FileHandler handler = new FileHandler(TEST_STATION_ID);
        handler.writeToFile("{\"msg\":\"hello\"}", 1, 0,0,"",0);

        // Lock main file to simulate failure in replace
        try (FileChannel ignored = FileChannel.open(MAIN_FILE, StandardOpenOption.WRITE)) {
            handler.writeToFile("{\"msg\":\"world\"}", 2, 0,0,"",0);
        } catch (IOException e) {
            // some OSs may not allow locking like this; fallback is to check stderr manually
        }

        // Ensure at least temp file exists even if move fails
        assertTrue(Files.exists(TEMP_FILE) || Files.exists(MAIN_FILE));
    }

    @Test
    void testReadFromFile_ValidFile_UpdatesState() {
        FileHandler writer = new FileHandler(TEST_STATION_ID);
        writer.writeToFile("{\"data\":42}", 7, 0,0,"",0);

        FileHandler reader = new FileHandler(TEST_STATION_ID);
        reader.readFromFile(TEST_STATION_ID);

        assertEquals(7, reader.getLamportClock());
        assertTrue(reader.getSerializedObj().contains("data"));
    }

    @Test
    void testReadFromFile_FileMissing_GracefulFailure() {
        FileHandler reader = new FileHandler(TEST_STATION_ID);
        assertDoesNotThrow(() -> reader.readFromFile(TEST_STATION_ID));
        assertNull(reader.getSerializedObj(), "Serialized object should remain null if no file");
    }

    @Test
    void testReadFromFile_CorruptedFile_GracefulFailure() throws IOException {
        Files.writeString(MAIN_FILE, "invalid_json");

        FileHandler reader = new FileHandler(TEST_STATION_ID);
        assertDoesNotThrow(() -> reader.readFromFile(TEST_STATION_ID));
        assertNull(reader.getSerializedObj(), "Should not crash on corrupted JSON");
    }

    @Test
    void testGetSerializedObj_ReturnsLatest() {
        FileHandler handler = new FileHandler(TEST_STATION_ID);
        handler.writeToFile("{\"abc\":123}", 3, 0,0,"",0);
        assertEquals("{\"abc\":123}", handler.getSerializedObj());
    }

    @Test
    void testGetSerializedObj_GreaterLamport_ReturnsLatest() {
        FileHandler handler = new FileHandler(TEST_STATION_ID);
        handler.writeToFile("{\"abc\":123}", 3, 0,0,"",0);
        handler.writeToFile("{\"msg\":\"hello\"}", 5, 0,0,"",0);

        assertEquals("{\"msg\":\"hello\"}", handler.getSerializedObj());
    }

    @Test
    void testGetSerializedObj_LowerLamport_ReturnsLatest() {
        FileHandler handler = new FileHandler(TEST_STATION_ID);
        handler.writeToFile("{\"abc\":123}", 3, 0,0,"",0);
        handler.writeToFile("{\"msg\":\"hello\"}", 1, 0,0,"",0);

        assertEquals("{\"abc\":123}", handler.getSerializedObj());
    }

    @Test
    void testGetSerializedObj_SameLamport_ReturnsLatest() {
        FileHandler handler = new FileHandler(TEST_STATION_ID);
        handler.writeToFile("{\"abc\":123}", 3, 0,0,"",0);
        handler.writeToFile("{\"msg\":\"hello\"}", 3, 0,0,"",0);

        assertEquals("{\"abc\":123}", handler.getSerializedObj());
    }

    @Test
    void testCrash_TempFileExistsButMainFileMissing() throws IOException {
        // Simulate crash: only temp file is left behind
        String tempContent = "{ \"lamport\": 99, \"body\": {\"msg\":\"crash-temp\"}}";
        Files.writeString(TEMP_FILE, tempContent);

        // Recovery attempt: readFromFile only reads main, so state should stay null
        FileHandler reader = new FileHandler(TEST_STATION_ID);
        reader.readFromFile(TEST_STATION_ID);

        assertNull(reader.getSerializedObj(),
                "readFromFile should ignore temp-only state since main file missing");
    }

    @Test
    void testCrash_TempFileIncompleteJson() throws IOException {
        // Simulate crash: half-written JSON in temp file
        String corrupted = "{ \"lamport\": 100, \"body\": {\"msg\":\"oops\""; // missing closing braces
        Files.writeString(TEMP_FILE, corrupted);

        // Then assume move never happened
        assertTrue(Files.exists(TEMP_FILE));
        assertFalse(Files.exists(MAIN_FILE));

        // Recovery attempt: still no main, so nothing should load
        FileHandler reader = new FileHandler(TEST_STATION_ID);
        reader.readFromFile(TEST_STATION_ID);

        assertNull(reader.getSerializedObj(),
                "Corrupted temp file should not affect recovery since main is missing");
    }

    @Test
    void testCrash_BothTempAndMainExist_MainPreferred() throws IOException {
        // Simulate crash after writing temp but before cleaning it up
        String mainContent = "{ \"meta\": {\"lamport\": 50, \"lastUpdated\": 0, \"updateCount\": 0, \"host\":\"127.0.0.1\", \"port\":10 }, \"body\": {\"msg\":\"main-ok\"}}";
        String tempContent = "{ \"meta\": {\"lamport\": 200, \"lastUpdated\": 0, \"updateCount\": 0, \"host\":\"127.0.0.1\", \"port\":10 }, \"body\": {\"msg\":\"temp-stale\"}}";

        Files.writeString(MAIN_FILE, mainContent);
        Files.writeString(TEMP_FILE, tempContent);

        FileHandler reader = new FileHandler(TEST_STATION_ID);
        reader.readFromFile(TEST_STATION_ID);

        // only reads MAIN file
        assertEquals("{\"msg\":\"main-ok\"}", reader.getSerializedObj());
    }

    @Test
    void testCrash_CorruptedMainFileButTempExists() throws IOException {
        // Simulate crash where main is corrupted but temp is intact
        Files.writeString(MAIN_FILE, "corrupted");
        Files.writeString(TEMP_FILE, "{ \"lamport\": 10, \"body\": {\"msg\":\"temp-fallback\"}}");

        FileHandler reader = new FileHandler(TEST_STATION_ID);
        reader.readFromFile(TEST_STATION_ID);

        // Since only main is read, this will fail gracefully
        assertNull(reader.getSerializedObj(),
                "Corrupted main should not load, even if temp is valid (current limitation)");
    }

    @Test
    void testConcurrentReadersAndWriter() throws Exception {
        FileHandler handler = new FileHandler(TEST_STATION_ID);

        // start a new executor  that runs upto 10 threads in parallel
        ExecutorService executor = Executors.newFixedThreadPool(10);
        // using a latch to execute the threads simultaneously
        // value is one, so that the first count-down releases threads.
        CountDownLatch startLatch = new CountDownLatch(1);
        // using a latch to see when the execution finishes
        // value is 20 for the 15 read and 5 writes
        CountDownLatch doneLatch = new CountDownLatch(20);

        // 15 readers
        IntStream.range(0, 15).forEach(
                i -> executor.submit(() -> {
                    // in each thread do the following
                    try {
                        // wait for latch to reach 0
                        startLatch.await();
                        // read 100 times
                        for (int j = 0; j < 100; j++) {
                            String obj = handler.getSerializedObj();
                            // the obj should either be null (not written to yet)
                            // or should have msg: something, i.e not corrupted by multiple writes
                            if (obj != null) {
                                assertTrue(obj.contains("msg"));
                            }
                        }
                    } catch (Exception ignored) {
                        // ignore exception
                    } finally {
                        // countdown on done latch
                        doneLatch.countDown();
                    }
                })
        );

        // 5 writers
        IntStream.range(0, 5).forEach(
                i -> executor.submit(() -> {
                    // for each thread do the following
                    try {
                        // wait for latch to reach 0
                        startLatch.await();
                        // write 20 times with different lamport values
                        for (int j = 0; j < 20; j++) {
                            handler.writeToFile("{\"msg\":\"writer-" + i + "-round-" + j + "\"}", j + i * 100, 0,0,"",0);
                        }
                    } catch (Exception ignored) {
                        // ignore exception
                    } finally {
                        // countdown on down latch
                        doneLatch.countDown();
                    }
                })
        );

        // start all threads
        startLatch.countDown();
        // the threads should finish in 10 seconds, making sure no deadlocks
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All tasks should finish in time");

        executor.shutdownNow();

        // Validate final state is as expected
        String finalObj = handler.getSerializedObj();
        assertNotNull(finalObj);
        assertTrue(finalObj.contains("writer-4-round-19"), "Final object should come from a writer");
        assertEquals(19+4*100, handler.getLamportClock());
    }

    @Test
    void testManyConcurrentWrites_OnlyHighestLamportSurvives() throws Exception {
        FileHandler handler = new FileHandler(TEST_STATION_ID);

        // run up to 8 threads in parallel
        ExecutorService executor = Executors.newFixedThreadPool(8);

        // Submit many concurrent writes with increasing lamport values
        IntStream.range(1,51).forEach(
                i -> executor.submit(() -> {
                    try {
                        handler.writeToFile("{\"msg\":\"val-" + i + "\"}", i, 0,0,"",0);
                    } catch (Exception ignored) {
                        // ignore exception
                    }
                })
        );

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        // After all writes, lamport must equal the max
        assertEquals(50, handler.getLamportClock());
        assertEquals("{\"msg\":\"val-50\"}", handler.getSerializedObj());
    }

    @Test
    void testWriteToFile_MetadataWrittenCorrectly() throws IOException {
        FileHandler handler = new FileHandler(TEST_STATION_ID);

        handler.writeToFile("{\"msg\":\"hello\"}", 5, 123456, 42,"127.0.0.1", 8080);

        assertTrue(Files.exists(MAIN_FILE), "Main file should exist after write");

        // Read back from file
        String content = Files.readString(MAIN_FILE);
        assertTrue(content.contains("\"lamport\": 5"));
        assertTrue(content.contains("\"updateCount\": 42"));
        assertTrue(content.contains("\"lastUpdated\": 123456"));
        assertTrue(content.contains("\"host\": \"127.0.0.1\""));
        assertTrue(content.contains("\"port\": 8080"));
    }

    @Test
    void testReadFromFile_MetadataRestored() throws IOException {
        // Write JSON manually with metadata
        String json = "{\n" +
                "  \"meta\": {\n" +
                "    \"lamport\": 10,\n" +
                "    \"lastUpdated\": 123456,\n" +
                "    \"updateCount\": 10,\n" +
                "    \"host\": \"127.0.0.1\",\n" +
                "    \"port\": 1234\n" +
                "  },\n" +
                "  \"body\": {\"msg\":\"hi\"}\n" +
                "}";
        Files.writeString(MAIN_FILE, json);

        FileHandler handler = new FileHandler(TEST_STATION_ID);
        handler.readFromFile(TEST_STATION_ID);

        assertEquals(10, handler.getLamportClock());
        assertEquals(123456, handler.getLastUpdated());
        assertEquals(10, handler.getGlobalUpdateCount());
        assertEquals("127.0.0.1", handler.getLastHost());
        assertEquals(1234, handler.getLastPort());
        assertTrue(handler.getSerializedObj().contains("\"msg\":\"hi\""));
    }

    @Test
    void testWriteToFile_OverwritesMetadata() throws IOException {
        FileHandler handler = new FileHandler(TEST_STATION_ID);

        handler.writeToFile("{\"msg\":\"first\"}", 1, 1,1, "127.0.0.1", 1111);
        handler.writeToFile("{\"msg\":\"second\"}", 2, 2,2, "127.0.0.2", 2222);

        String content = Files.readString(MAIN_FILE);
        assertTrue(content.contains("\"msg\":\"second\""));
        assertTrue(content.contains("\"updateCount\": 2"));
        assertTrue(content.contains("\"lastUpdated\": 2"));
        assertTrue(content.contains("\"host\": \"127.0.0.2\""));
        assertTrue(content.contains("\"port\": 2222"));
    }

    @Test
    void testGettersThreadSafety() {
        FileHandler handler = new FileHandler(TEST_STATION_ID);
        handler.writeToFile("{\"msg\":\"safe\"}", 3, 7,10, "localhost", 9999);

        // Call getters under multiple threads
        IntStream.range(0, 20).parallel().forEach(i -> {
            assertNotNull(handler.getSerializedObj());
            assertTrue(handler.getLamportClock() >= 0);
            assertTrue(handler.getGlobalUpdateCount() >= 0);
            assertNotNull(handler.getLastHost());
            assertTrue(handler.getLastPort() > 0);
        });
    }
}


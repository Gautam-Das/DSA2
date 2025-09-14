package ContentServer;

import AggregationServer.AggregationServer;
import org.junit.jupiter.api.*;
import shared.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class ContentServerTest {

    private AggregationServer server;
    private ExecutorService executor;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        // get a free port
        try (ServerSocket ss = new ServerSocket(0)) {
            port = ss.getLocalPort();
        }

        server = new AggregationServer(port);
        executor = Executors.newSingleThreadExecutor();
        executor.submit(server::run);

        // wait briefly for server to start
        Thread.sleep(200);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void testSingleEntryCreatesFile() throws Exception {
        // Prepare single-entry file
        File tmp = File.createTempFile("singleEntry", ".txt");
        try (PrintWriter pw = new PrintWriter(tmp)) {
            pw.println("id:SINGLE");
            pw.println("temp:21");
        }

        ContentServer cs = new ContentServer("127.0.0.1", port, tmp.getAbsolutePath());
        cs.connectToServer();
        cs.syncWithServer();
        cs.run();
        cs.closeConnection();

        File created = new File("SINGLE.json");
        assertTrue(created.exists(), "File for SINGLE station should be created on server");
    }

    @Test
    void testDoubleEntryCreatesTwoFiles() throws Exception {
        // Prepare double-entry file
        File tmp = File.createTempFile("doubleEntry", ".txt");
        try (PrintWriter pw = new PrintWriter(tmp)) {
            pw.println("id:F1");
            pw.println("val:100");
            pw.println("id:F2");
            pw.println("val:200");
        }

        ContentServer cs = new ContentServer("127.0.0.1", port, tmp.getAbsolutePath());
        cs.connectToServer();
        cs.syncWithServer();
        cs.run();
        cs.closeConnection();

        File f1 = new File("F1.json");
        File f2 = new File("F2.json");
        assertTrue(f1.exists(), "F1.json should exist");
        assertTrue(f2.exists(), "F2.json should exist");
    }

    @Test
    void testLamportClockSyncUpdatesCorrectly() throws Exception {
        ContentServer cs = new ContentServer("127.0.0.1", port, "dummy.txt");
        cs.connectToServer();

        // local clock starts at 0
        assertEquals(0, cs.lamportClock);

        cs.syncWithServer();

        // After sync, lamportClock should be >= 1
        assertTrue(cs.lamportClock >= 1, "Lamport clock should increment after sync");

        cs.closeConnection();
    }

    @Test
    void testRetriesOnServerDown() throws Exception {
        // Stop current server
        executor.shutdownNow();
        Thread.sleep(200);

        // Start a new server but later
        executor = Executors.newSingleThreadExecutor();
        new Thread(() -> {
            try {
                Thread.sleep(1500); // simulate downtime
                server = new AggregationServer(port);
                server.run();
            } catch (Exception ignored) {}
        }).start();

        // Prepare single-entry file
        File tmp = File.createTempFile("retryEntry", ".txt");
        try (PrintWriter pw = new PrintWriter(tmp)) {
            pw.println("id:RETRY");
            pw.println("temp:42");
        }

        ContentServer cs = new ContentServer("127.0.0.1", port, tmp.getAbsolutePath());

        // connect will retry until server is back
        cs.connectToServer();
        cs.syncWithServer();
        cs.run();
        cs.closeConnection();

        File created = new File("RETRY.json");
        assertTrue(created.exists(), "File for RETRY station should be created after retries");
    }
}

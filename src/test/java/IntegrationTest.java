import AggregationServer.AggregationServer;
import ContentServer.ContentServer;
import GETclient.GETClient;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class IntegrationTest {

    private AggregationServer server;
    private ExecutorService executor;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        try (ServerSocket ss = new ServerSocket(0)) {
            port = ss.getLocalPort();
        }
        server = new AggregationServer(port);
        executor = Executors.newSingleThreadExecutor();
        executor.submit(server::run);

        Thread.sleep(200); // wait for server startup
    }

    @AfterEach
    void tearDown() throws Exception {
        executor.shutdownNow();
    }

    @Test
    void testMultipleContentServersAndGetClient() throws Exception {
        // Prepare two sample data files
        File f1 = new File("CS1.txt");
        try (PrintWriter pw = new PrintWriter(f1)) {
            pw.println("id:G1");
            pw.println("temp:20");
        }

        File f2 = new File("CS2.txt");
        try (PrintWriter pw = new PrintWriter(f2)) {
            pw.println("id:G2");
            pw.println("temp:30");
        }

        // Start two ContentServers in parallel
        ExecutorService csExec = Executors.newFixedThreadPool(2);
        // submit te tasks for both
        csExec.submit(() -> {
            try {
                ContentServer cs1 = new ContentServer("127.0.0.1", port, "CS1.txt");
                cs1.connectToServer();
                cs1.syncWithServer();
                cs1.run();
            } catch (Exception e) {
                fail("ContentServer1 failed: " + e.getMessage());
            }
        });
        csExec.submit(() -> {
            try {
                ContentServer cs2 = new ContentServer("127.0.0.1", port, "CS2.txt");
                cs2.connectToServer();
                cs2.syncWithServer();
                cs2.run();
            } catch (Exception e) {
                fail("ContentServer2 failed: " + e.getMessage());
            }
        });
        csExec.shutdown();

        Thread.sleep(1000); // allow servers to push data

        System.out.println("Finished uploading----");
        PrintStream originalOut = System.out;


        // start 10 GET clients in parallel
        ExecutorService gcExec = Executors.newFixedThreadPool(10);
        ArrayList<Future<String>> results = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            results.add(gcExec.submit(() -> {
                GETClient client = new GETClient("127.0.0.1", port, null);
                client.connectToServer();
                client.syncWithServer();
                String res =  client.run();
                client.closeConnection();

                return res;
            }));
        }

        gcExec.shutdown();
        gcExec.awaitTermination(5, TimeUnit.SECONDS);
        System.setOut(originalOut);

        // Assertions
        for (Future<String> f : results) {
            String output = f.get();
            assertTrue(output.contains("G1"), "Each client should see station G1");
            assertTrue(output.contains("G2"), "Each client should see station G2");
        }
    }
}

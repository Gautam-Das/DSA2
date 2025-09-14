package GETclient;

import AggregationServer.AggregationServer;
import org.junit.jupiter.api.*;
import shared.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class GETClientTest {

    private AggregationServer server;
    private ExecutorService executor;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        // use random port
        try (ServerSocket ss = new ServerSocket(0)) {
            port = ss.getLocalPort();
        }
        server = new AggregationServer(port);
        executor = Executors.newSingleThreadExecutor();
        executor.submit(server::run);
        Thread.sleep(200); // wait for server
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
        // cleanup files
        File[] files = new File(".").listFiles((dir, name) -> name.endsWith(".json"));
        if (files != null) {
            for (File f : files) f.delete();
        }
    }

    private void sendPut(String id, String key, String val, int lamport) throws Exception {
        HTTPParser parser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Lamport-Clock", String.valueOf(lamport));
        HashMap<String, String> body = new HashMap<>();
        body.put("id", id);
        body.put(key, val);
        String req = parser.createHTTPRequest("PUT", "/weather.json", body, headers);

        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out.writeUTF(req);
        in.readUTF(); // ignore response
    }

    @Test
    void testGetAllReturnsExpected() throws Exception {
        sendPut("G1", "temp", "20", 1);
        sendPut("G2", "temp", "25", 2);

        GETClient client = new GETClient("127.0.0.1", port, null);
        client.connectToServer();
        client.syncWithServer();

        // create new output stream, and set it as the default output
        // this way the client prints to this stream instead of standard output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(baos));

        client.run();

        // reset to original
        System.setOut(originalOut);
        String output = baos.toString();

        assertTrue(output.contains("G1"));
        assertTrue(output.contains("G2"));

        client.closeConnection();
    }

    @Test
    void testGetSpecificStation() throws Exception {
        sendPut("SPECIFIC", "humidity", "50", 3);

        GETClient client = new GETClient("127.0.0.1", port, "SPECIFIC");
        client.connectToServer();
        client.syncWithServer();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(baos));

        client.run();

        System.setOut(originalOut);
        String output = baos.toString();

        assertTrue(output.contains("SPECIFIC"));
        assertTrue(output.contains("humidity"));

        client.closeConnection();
    }

    @Test
    void testGetInvalidStationReturns400() throws Exception {
        GETClient client = new GETClient("127.0.0.1", port, "DOESNOTEXIST");
        client.connectToServer();
        client.syncWithServer();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(baos));

        client.run();

        System.setOut(originalOut);
        String output = baos.toString();

        assertTrue(output.contains("400 Bad Request"));

        client.closeConnection();
    }

    @Test
    void testLamportClockIncrementsOnGet() throws Exception {
        sendPut("LAMPORT", "val", "1", 5);

        GETClient client = new GETClient("127.0.0.1", port, "LAMPORT");
        client.connectToServer();
        int before = client.lamportClock;
        client.syncWithServer();
        client.run();
        int after = client.lamportClock;

        assertTrue(after > before, "Lamport clock should increment after GET");

        client.closeConnection();
    }
}

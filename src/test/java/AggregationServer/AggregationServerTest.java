package AggregationServer;

import org.junit.jupiter.api.*;
import shared.*;

import java.io.*;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class AggregationServerTest {

    private AggregationServer server;
    private ExecutorService executor;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        // use random free port
        try (ServerSocket ss = new ServerSocket(0)) {
            port = ss.getLocalPort();
        }


        server = new AggregationServer(port);
        executor = Executors.newSingleThreadExecutor();
        executor.submit(server::run);

        // small wait to ensure server is up
        Thread.sleep(200);
    }

    @Test
    void testGetWithNoDataReturnsEmptyArray() throws Exception {
        HTTPParser parser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Lamport-Clock", "1");

        String req = parser.createHTTPRequest("GET", "/", null, headers);
        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out.writeUTF(req);

        HTTPResponse httpResponse = parser.parseHttpResponse(in.readUTF());
        assertEquals(200, httpResponse.statusCode);
        assertEquals("[]", httpResponse.body, "GET / on empty server should return []");
        socket.close();
    }

    @Test
    void testPutCreatesFileAndReturns201() throws Exception {
        HTTPParser parser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Lamport-Clock", "1");

        HashMap<String, String> body = new HashMap<>();
        body.put("id", "STATION1");
        body.put("temp", "25");

        String request = parser.createHTTPRequest("PUT", "/weather.json", body, headers);
        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out.writeUTF(request);
        String response = in.readUTF();

        HTTPResponse httpResponse = parser.parseHttpResponse(response);
        assertNotNull(httpResponse);
        assertEquals(201, httpResponse.statusCode);

        File created = new File("STATION1.json");
        assertTrue(created.exists(), "File should be created for station");

        socket.close();
    }

    @Test
    void testPutThenGetReturnsSameData() throws Exception {
        HTTPParser parser = new HTTPParser();

        // First PUT
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Lamport-Clock", "2");
        HashMap<String, String> body = new HashMap<>();
        body.put("id", "STATION2");
        body.put("humidity", "55");
        String putReq = parser.createHTTPRequest("PUT", "/weather.json", body, headers);
        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out.writeUTF(putReq);
        String putResp = in.readUTF();
        assertEquals(201, parser.parseHttpResponse(putResp).statusCode);

        // Then GET
        headers = new HashMap<>();
        headers.put("Lamport-Clock", "3");
        String getReq = parser.createHTTPRequest("GET", "/STATION2", null, headers);
        Socket getSocket = new Socket("127.0.0.1", port);
        DataOutputStream getOutStream = new DataOutputStream(getSocket.getOutputStream());
        DataInputStream getInStream = new DataInputStream(new BufferedInputStream(getSocket.getInputStream()));
        getOutStream.writeUTF(getReq);
        String getResp = getInStream.readUTF();

        HTTPResponse httpResponse = parser.parseHttpResponse(getResp);
        assertNotNull(httpResponse);
        assertEquals(200, httpResponse.statusCode);
        assertTrue(httpResponse.body.contains("STATION2"));
        assertTrue(httpResponse.body.contains("humidity"));
    }

    @Test
    void testGetAllReturnsMultipleEntries() throws Exception {
        HTTPParser parser = new HTTPParser();

        // Add two stations
        for (int i = 1; i <= 2; i++) {
            HashMap<String, String> headers = new HashMap<>();
            headers.put("Lamport-Clock", String.valueOf(i));
            HashMap<String, String> body = new HashMap<>();
            body.put("id", "S" + i);
            body.put("val", String.valueOf(10 * i));
            String putReq = parser.createHTTPRequest("PUT", "/weather.json", body, headers);

            Socket socket = new Socket("127.0.0.1", port);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            out.writeUTF(putReq);
            String putResp = in.readUTF();
        }

        // GET all
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Lamport-Clock", "5");
        String getReq = parser.createHTTPRequest("GET", "/", null, headers);
        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out.writeUTF(getReq);
        String resp = in.readUTF();

        HTTPResponse httpResponse = parser.parseHttpResponse(resp);
        assertEquals(200, httpResponse.statusCode);
        assertTrue(httpResponse.body.startsWith("["));
        assertTrue(httpResponse.body.contains("S1"));
        assertTrue(httpResponse.body.contains("S2"));
    }

    @Test
    void testSyncIncrementsLamport() throws Exception {
        HTTPParser parser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Lamport-Clock", "7");
        String req = parser.createHTTPRequest("SYNC", "/", null, headers);

        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out.writeUTF(req);
        String resp = in.readUTF();
        HTTPResponse httpResponse = parser.parseHttpResponse(resp);
        assertEquals(200, httpResponse.statusCode);

        assertNotNull(httpResponse.headers.get("Lamport-Clock"));
        int lamport = Integer.parseInt(httpResponse.headers.get("Lamport-Clock").trim());
        assertTrue(lamport >= 8); // should be bumped
    }

    @Test
    void testBadRequestOnMalformedHttp() throws Exception {
        String badReq = "INVALID_REQUEST\r\n";
        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out.writeUTF(badReq);
        String resp = in.readUTF();

        HTTPParser parser = new HTTPParser();
        HTTPResponse httpResponse = parser.parseHttpResponse(resp);

        assertNotNull(httpResponse);
        assertEquals(400, httpResponse.statusCode);
    }

    @Test
    void testInvalidUriReturns400() throws Exception {
        HTTPParser parser = new HTTPParser();
        String req = "GET /foo/bar HTTP/1.1\r\nLamport-Clock: 1\r\n\r\n";

        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

        out.writeUTF(req);
        HTTPResponse httpResponse = parser.parseHttpResponse(in.readUTF());

        assertEquals(400, httpResponse.statusCode);
        socket.close();
    }

    @Test
    void testMissingLamportClockReturns400() throws Exception {
        HTTPParser parser = new HTTPParser();
        String req = "GET /STATION1 HTTP/1.1\r\n\r\n";

        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

        out.writeUTF(req);
        HTTPResponse httpResponse = parser.parseHttpResponse(in.readUTF());

        assertEquals(400, httpResponse.statusCode);
        socket.close();
    }

    @Test
    void testPutWithoutIdReturns400() throws Exception {
        HTTPParser parser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Lamport-Clock", "5");
        HashMap<String, String> body = new HashMap<>();
        body.put("temp", "22");

        String req = parser.createHTTPRequest("PUT", "/weather.json", body, headers);

        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out.writeUTF(req);

        HTTPResponse httpResponse = parser.parseHttpResponse(in.readUTF());
        assertEquals(400, httpResponse.statusCode);
    }

    @Test
    void testPutEmptyBodyReturns204() throws Exception {
        String req = "PUT /weather.json HTTP/1.1\r\nLamport-Clock: 1\r\n\r\n";

        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

        out.writeUTF(req);
        HTTPResponse httpResponse = new HTTPParser().parseHttpResponse(in.readUTF());

        assertEquals(204, httpResponse.statusCode);
    }

    @Test
    void testPutCorruptedJsonReturns500() throws Exception {
        String req = "PUT /weather.json HTTP/1.1\r\nLamport-Clock: 1\r\n\r\n{id: ,}";

        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

        out.writeUTF(req);
        HTTPResponse httpResponse = new HTTPParser().parseHttpResponse(in.readUTF());

        assertEquals(500, httpResponse.statusCode);
    }

    @Test
    void testGetInvalidStationReturns400() throws Exception {
        HTTPParser parser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Lamport-Clock", "5");
        String req = parser.createHTTPRequest("GET", "/DOESNOTEXIST", null, headers);

        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out.writeUTF(req);

        HTTPResponse httpResponse = parser.parseHttpResponse(in.readUTF());
        assertEquals(400, httpResponse.statusCode);
    }

    // concurrency test with lamport clock
    @Test
    void testConcurrentPutsLamportMonotonic() throws Exception {
        int clients = 5;
        ExecutorService executor = Executors.newFixedThreadPool(clients);
        AtomicInteger success = new AtomicInteger(0);

        for (int i = 0; i < clients; i++) {
            int idx = i;
            executor.submit(() -> {
                try {
                    HTTPParser parser = new HTTPParser();
                    HashMap<String, String> headers = new HashMap<>();
                    headers.put("Lamport-Clock", String.valueOf(idx + 1));
                    HashMap<String, String> body = new HashMap<>();
                    body.put("id", "C" + idx);
                    body.put("val", String.valueOf(idx));

                    String req = parser.createHTTPRequest("PUT", "/weather.json", body, headers);
                    Socket socket = new Socket("127.0.0.1", port);
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                    out.writeUTF(req);
                    HTTPResponse httpResponse = parser.parseHttpResponse(in.readUTF());
                    if (httpResponse.statusCode == 201 || httpResponse.statusCode == 200) {
                        success.incrementAndGet();
                    }
                } catch (Exception ignored) {}
            });
        }

        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.SECONDS);

        assertEquals(clients, success.get(), "All clients should succeed");
        assertTrue(AggregationServer.ServerLamportClock >= clients);
    }

    // concurrency test with update count
    @Test
    void testConcurrentIncrementUpdateCount() throws Exception {
        int clients = 5;
        ExecutorService executor = Executors.newFixedThreadPool(clients);
        AtomicInteger success = new AtomicInteger(0);
        int updateCountBefore = AggregationServer.ServerUpdateCount;
        for (int i = 0; i < clients; i++) {
            int idx = i;
            executor.submit(() -> {
                try {
                    HTTPParser parser = new HTTPParser();
                    HashMap<String, String> headers = new HashMap<>();
                    headers.put("Lamport-Clock", String.valueOf(idx + 1));
                    HashMap<String, String> body = new HashMap<>();
                    body.put("id", "C" + idx);
                    body.put("val", String.valueOf(idx));

                    String req = parser.createHTTPRequest("PUT", "/weather.json", body, headers);
                    Socket socket = new Socket("127.0.0.1", port);
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                    out.writeUTF(req);
                    HTTPResponse httpResponse = parser.parseHttpResponse(in.readUTF());
                    if (httpResponse.statusCode == 201 || httpResponse.statusCode == 200) {
                        success.incrementAndGet();
                    }
                } catch (Exception ignored) {}
            });
        }

        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.SECONDS);

        assertEquals(clients, success.get(), "All clients should succeed");
        assertEquals(clients, AggregationServer.ServerUpdateCount - updateCountBefore);
    }

    @Test
    void testDisconnectDeletesFile() throws Exception {
        HTTPParser parser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Lamport-Clock", "1");
        HashMap<String, String> body = new HashMap<>();
        body.put("id", "DISC");
        body.put("val", "10");

        String req = parser.createHTTPRequest("PUT", "/weather.json", body, headers);
        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out.writeUTF(req);
        in.readUTF();

        File f = new File("DISC.json");
        assertTrue(f.exists());

        // disconnect
        socket.close();
        Thread.sleep(200);

        assertFalse(f.exists(), "File should be deleted after disconnect");
    }

    @Test
    void testPersistenceReloadsFiles() throws Exception {
        // create file manually
        FileHandler handler = new FileHandler("PERSIST");
        handler.writeToFile("{\"id\":\"PERSIST\"}", 5, System.currentTimeMillis(), 10, "127.0.0.1", 1234);

        // restart server
        executor.shutdownNow();
        server = new AggregationServer(port+1);
        executor = Executors.newSingleThreadExecutor();
        executor.submit(server::run);
        Thread.sleep(200);

        assertTrue(server.files.containsKey("PERSIST"));
        assertEquals(5, server.files.get("PERSIST").getLamportClock());
        assertEquals(10, server.files.get("PERSIST").getGlobalUpdateCount());
    }

    @Test
    void testLamportClockBasic() throws Exception {
        HTTPParser parser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Lamport-Clock", "5");

        String req = parser.createHTTPRequest("SYNC", "/", null, headers);
        Socket socket = new Socket("127.0.0.1", port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out.writeUTF(req);

        HTTPResponse httpResponse = parser.parseHttpResponse(in.readUTF());
        assertEquals(200, httpResponse.statusCode);

        assertNotNull(httpResponse.headers.get("Lamport-Clock"));
        int lamport = Integer.parseInt(httpResponse.headers.get("Lamport-Clock").trim());
        assertTrue(lamport >= 6, "Server Lamport clock should advance beyond client’s");
        socket.close();
    }



    @Test
    void testConcurrentGetWhilePut() throws Exception {
        HTTPParser parser = new HTTPParser();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        // PUT task
        Runnable putTask = () -> {
            try {
                HashMap<String, String> headers = new HashMap<>();
                headers.put("Lamport-Clock", "10");
                HashMap<String, String> body = new HashMap<>();
                body.put("id", "CONCURRENT1");
                body.put("val", "999");

                String req = parser.createHTTPRequest("PUT", "/weather.json", body, headers);
                Socket socket = new Socket("127.0.0.1", port);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                out.writeUTF(req);
                HTTPResponse resp = parser.parseHttpResponse(in.readUTF());
                assertTrue(resp.statusCode == 200 || resp.statusCode == 201);
                socket.close();
            } catch (Exception e) {
                fail("PUT failed: " + e.getMessage());
            }
        };

        // GET task
        Runnable getTask = () -> {
            try {
                HashMap<String, String> headers = new HashMap<>();
                headers.put("Lamport-Clock", "11");
                String req = parser.createHTTPRequest("GET", "/CONCURRENT1", null, headers);
                Socket socket = new Socket("127.0.0.1", port);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                out.writeUTF(req);
                HTTPResponse resp = parser.parseHttpResponse(in.readUTF());
                // Either 200 (if PUT finished) or 400 (if not yet) is valid
                assertTrue(resp.statusCode == 200 || resp.statusCode == 400);
                socket.close();
            } catch (Exception e) {
                fail("GET failed: " + e.getMessage());
            }
        };

        executor.submit(putTask);
        executor.submit(getTask);

        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.SECONDS);
    }

    @Test
    void testPersistenceDeletesFileAfter2Mins() throws Exception {
        // create file manually
        FileHandler handler = new FileHandler("PERSIST");
        handler.writeToFile("{\"id\":\"PERSIST\"}", 5, System.currentTimeMillis(), 10, "127.0.0.1", 1234);

        // restart server
        executor.shutdownNow();
        server = new AggregationServer(port+1);
        executor = Executors.newSingleThreadExecutor();
        executor.submit(server::run);
        Thread.sleep(200);

        assertTrue(server.files.containsKey("PERSIST"));
        assertEquals(5, server.files.get("PERSIST").getLamportClock());
        assertEquals(10, server.files.get("PERSIST").getGlobalUpdateCount());

        Thread.sleep(130000); // sleep more than 2 mins

        assertFalse(server.files.containsKey("PERSIST"));

        File f = new File("PERSIST.json");
        assertFalse(f.exists(), "File should be deleted after 2 mins");
    }
}

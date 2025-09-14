package AggregationServer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import shared.FileHandler;
import shared.HTTPParser;
import shared.HTTPRequest;
import shared.UrlManager;

/**
 * The {@code AggregationServer} accepts connections from ContentServers and GETClients.
 * It stores incoming weather station data (PUT requests) into JSON files, handles
 * GET requests for current data, and synchronizes Lamport clocks across distributed services.
 * <br></br>
 * Features:
 * Maintains Lamport clock for causal ordering
 * Persists station data into JSON files with update counts
 * Handles expiration of outdated files via a garbage collection thread
 * Reloads persisted files and restores clocks after server restart
 */
public class AggregationServer {

    private ServerSocket serverSocket;
    ConcurrentHashMap<String, FileHandler> files = new ConcurrentHashMap<>();
    public static int ServerLamportClock = 0;
    public static int ServerUpdateCount = 0;

    int GC_THREAD_SLEEP_TIME = 120_000;
    /**
     * Creates an {@code AggregationServer} bound to the specified port.
     *
     * @param port the port to bind the server socket
     */
    public AggregationServer(int port) {
        try {
            serverSocket = new ServerSocket(port);
            System.out.printf("Server connected to Socket on port %d\n", port);
        } catch (IOException i) {
            System.out.println(i.getMessage());
        }
    }

    /**
     * Loads existing JSON files from the working directory into memory.
     * <p>
     * - Ignores temporary files (ending with {@code -temp.json}).<br>
     * - Updates {@code ServerLamportClock} and {@code ServerUpdateCount}
     *   based on the maximum values found in files.
     * </p>
     */
    public void getExistingFiles() {
        File dir = new File(".");
        File[] dirFiles = dir.listFiles();
        if (dirFiles != null) {
            int maxUpdateCount = 0;
            int maxLamport = 0;

            for (File file : dirFiles) {
                String fileName = file.getName();
                if (!fileName.endsWith(".json") || fileName.endsWith("-temp.json")) continue;
                System.out.println("found file: " + fileName);
                String stationId = fileName.replace(".json", "");
                FileHandler handler = new FileHandler(stationId);
                handler.readFromFile(stationId);
                files.put(stationId, handler);

                // track max counters
                maxUpdateCount = Math.max(maxUpdateCount, handler.getGlobalUpdateCount());
                maxLamport = Math.max(maxLamport, handler.getLamportClock());

                System.out.printf("Loaded file for station %s (lamport=%d, updateCount=%d)\n",
                        stationId, handler.getLamportClock(), handler.getGlobalUpdateCount());
            }
            // update server counters
            ServerUpdateCount = maxUpdateCount;
            ServerLamportClock = maxLamport;
        }
        System.out.printf("Starting server with initial lamport: %d , updateCount: %d \n",
                ServerLamportClock, ServerUpdateCount);
    }

    /**
     * Starts the AggregationServer:
     * <ul>
     *   <li>Runs a garbage collector thread every 2 minutes to remove expired files</li>
     *   <li>Accepts incoming client connections</li>
     *   <li>Delegates each connection to a {@link handleConnection} thread</li>
     * </ul>
     *
     * <p>This method blocks indefinitely until terminated.</p>
     */
    public void run() {
        getExistingFiles();

        // Garbage collection thread to delete files
        new Thread(() -> {
            while (true) {
                try {
                    // runs every 2 minutes
                    Thread.sleep(GC_THREAD_SLEEP_TIME);

                    // check every current file handler object
                    for (ConcurrentHashMap.Entry<String, FileHandler> entry : files.entrySet()) {
                        String id = entry.getKey();
                        FileHandler handler = entry.getValue();

                        // using write lock to avoid having the object updated while deleting
                        handler.rwLock.writeLock().lock();
                        try {
                            if (handler.isExpired(ServerUpdateCount)) {
                                handler.deleteFileFromDisk();
                                files.remove(id, handler);
                                System.out.println("Deleted expired file: " + id);
                            }
                        } finally {
                            handler.rwLock.writeLock().unlock();
                        }
                    }
                } catch (InterruptedException ignored) {}
            }
        }).start();

        // Main loop
        while (true) {
            try {
                System.out.println("... waiting for connection");
                Socket socket = serverSocket.accept();
                System.out.printf("client / server from port %d connected\n", socket.getPort());
                new Thread(new handleConnection(socket, files)).start();
            } catch (IOException | NullPointerException i) {
                System.out.println(i.getMessage());
            }
        }
    }

    /**
     * Entry point for starting the AggregationServer.
     *
     * @param args command-line arguments, expected format: {@code -p <port>}
     */
    public static void main(String[] args) {
        // check params
        if (args.length != 2 || !args[0].equals("-p")) {
            System.err.println("Usage: java AggregationServer -p <port>");
            System.exit(1);
        }

        // get port
        String portStr = args[1];
        int port = UrlManager.validateAndGetPort(portStr);
        if (port == -1) {
            System.exit(1);
        }

        // start server
        AggregationServer aggregationServer = new AggregationServer(port);
        aggregationServer.run();
    }
}

/**
 * The {@code handleConnection} class manages a client connection to the
 * {@link AggregationServer}. Each instance runs in its own thread and is responsible for
 * parsing requests, updating Lamport clocks, and returning appropriate responses.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Parses and validates HTTP-like requests (GET, PUT, SYNC).</li>
 *   <li>Updates the global Lamport clock for each request.</li>
 *   <li>Handles creation, update, and expiration of weather station files.</li>
 *   <li>Cleans up resources when a ContentServer disconnects.</li>
 * </ul>
 */
class handleConnection implements Runnable {
    private Socket socket;
    private ConcurrentHashMap<String, FileHandler> files;
    // used when content server connects to track which station id it is reporting for
    private String contentServerStationId;

    private static final Object clockLock = new Object();

    /**
     * Constructs a new handler for a client connection.
     *
     * @param socket the client socket to handle
     * @param files  a shared map of station IDs to {@link FileHandler} objects
     */
    handleConnection(Socket socket, ConcurrentHashMap<String, FileHandler> files) {
        this.socket = socket;
        this.files = files;
    }

    /**
     * Main execution loop for handling client requests.
     *
     * <p>Behavior:</p>
     * <ul>
     *   <li>Blocks while waiting for messages from the client.</li>
     *   <li>Delegate requests to the appropriate handler method
     *       ({@code handleGETRequest}, {@code handlePUTRequest}, {@code handleSYNCRequest}).</li>
     *   <li>Handles malformed requests with a {@code 400 Bad Request} response.</li>
     *   <li>Deletes data files if a ContentServer disconnects unexpectedly.</li>
     * </ul>
     */
    @Override
    public void run() {
        try (
                DataInputStream inputStream = new DataInputStream(
                        new BufferedInputStream(socket.getInputStream())
                );
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())
        ) {
            HTTPParser parser = new HTTPParser();
            while (true) {
                String message;
                try {
                    // blocks until client sends another message or disconnects
                    message = inputStream.readUTF();
                } catch (IOException eof) {
                    handleDisconnect();
                    break; // exit loop and close socket
                }

                System.out.println("Received message:\n" + message);
                HTTPRequest request = parser.parseHttpRequest(message);

                String response = null;
                if (request == null) {
                    System.err.println("Parsing failed on message");
                } else if ("GET".equals(request.method)) {
                    response = handleGETRequest(request);
                } else if ("PUT".equals(request.method)) {
                    response = handlePUTRequest(request);
                } else if ("SYNC".equals(request.method)) {
                    response = handleSYNCRequest(request);
                } else {
                    System.err.println("Invalid HTTP method: " + request.method);
                }

                if (response == null) {
                    HashMap<String, String> headers = new HashMap<>();
                    synchronized (clockLock) {
                        AggregationServer.ServerLamportClock++;
                        headers.put("Lamport-Clock", String.valueOf(AggregationServer.ServerLamportClock));
                    }
                    response = parser.createHTTPResponse(400, "Bad Request", null, headers);
                }

                System.out.printf("Sending response to socket %d:\n%s\n", socket.getPort(), response);
                outputStream.writeUTF(response);
                outputStream.flush();
            }
        } catch (IOException i) {
            System.err.println("Error while handling connection: " + i.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    /**
     * Handles cleanup logic when a ContentServer disconnects.
     * If the disconnected client was responsible for a station ID,
     * the associated file is deleted and removed from the in-memory map.
     */
    private void handleDisconnect() {
        if (contentServerStationId != null) {
            FileHandler handler = files.get(contentServerStationId);
            if (handler == null ||
                    !socket.getInetAddress().getHostAddress().equals(handler.getLastHost()) ||
                    handler.getLastPort() != socket.getPort()) {
                return;
            }
            handler.rwLock.writeLock().lock();
            try {
                handler.deleteFileFromDisk();
                files.remove(contentServerStationId, handler);
                System.out.printf("Client %s:%d disconnected\n",
                        socket.getInetAddress().getHostAddress(),
                        socket.getPort());
                System.out.printf("deleted %s.json\n", contentServerStationId);
            } finally {
                handler.rwLock.writeLock().unlock();
            }
        }
    }

    /**
     * Helper class for GET request validation results.
     */
    private static class GETValidationResult {
        int status = 200;
        String message = "OK";
        String id = null;
        int requestLamportClock = 0;
    }

    /**
     * Validates a GET request for proper URI and Lamport clock header.
     *
     * @param request the parsed {@link HTTPRequest} object
     * @return a {@link GETValidationResult} containing validation status, ID (if any),
     *         and request Lamport clock
     */
    private GETValidationResult validateGETRequest(HTTPRequest request) {
        GETValidationResult result = new GETValidationResult();

        // URI if form /[id]
        // regex: ^/ starts with /
        // [^/]* any character any number of times except ^/
        // $ end of string
        if (!request.uri.matches("^/[^/]*$")) {
            result.status = 400;
            result.message = "Bad Request";
            return result;
        }

        String requestLamportClockString = request.headers.get("Lamport-Clock");
        if (requestLamportClockString == null) {
            result.status = 400;
            result.message = "Bad Request";
            return result;
        }

        try {
            result.requestLamportClock = Integer.parseInt(requestLamportClockString.trim());
        } catch (NumberFormatException e) {
            result.status = 400;
            result.message = "Bad Request";
            return result;
        }

        String id = request.uri.replace("/", "");
        if (!id.isEmpty()) {
            result.id = id; // single resource requested
        }

        return result;
    }

    /**
     * Handles GET requests.
     *
     * <p>Behavior:</p>
     * <ul>
     *   <li>If URI = "/", returns a JSON array of all valid station files.</li>
     *   <li>If URI = "/stationId", returns that station’s JSON or 400 if not found/expired.</li>
     *   <li>Always updates the server Lamport clock.</li>
     * </ul>
     *
     * @param request the parsed {@link HTTPRequest}
     * @return a serialized HTTP-like response string
     */
    private String handleGETRequest(HTTPRequest request) {
        HTTPParser httpParser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();

        GETValidationResult getValidationResult = validateGETRequest(request);

        // update server lamport clock value
        synchronized (clockLock) {
            AggregationServer.ServerLamportClock =
                    Math.max(AggregationServer.ServerLamportClock, getValidationResult.requestLamportClock) + 1;
            headers.put("Lamport-Clock", String.valueOf(AggregationServer.ServerLamportClock));
        }

        if (getValidationResult.status != 200) {
            return httpParser.createHTTPResponse(getValidationResult.status, getValidationResult.message, null, headers);
        }

        if (getValidationResult.id == null) {
            // return all in hashmap
            StringBuilder bodyBuilder = new StringBuilder();
            bodyBuilder.append("[");

            int count = 0;
            for (FileHandler handler : files.values()) {
                if (handler.isExpired(AggregationServer.ServerUpdateCount)) continue;
                if (count > 0) bodyBuilder.append(",");
                count++;
                bodyBuilder.append(handler.getSerializedObj());
            }
            bodyBuilder.append("]");
            return httpParser.createHTTPResponse(200, "OK", bodyBuilder.toString(), headers);
        } else {
            // return the one matching id else bad request
            FileHandler fileHandler = files.get(getValidationResult.id);
            if (fileHandler == null || fileHandler.isExpired(AggregationServer.ServerUpdateCount)) {
                return httpParser.createHTTPResponse(400, "Bad Request", null, headers);
            }
            return httpParser.createHTTPResponse(200, "OK", fileHandler.getSerializedObj(), headers);
        }
    }

    /**
     * Helper class for PUT request validation results.
     */
    private static class PUTValidationResult {
        int status = 200;
        int requestLamportClock = 0;
        String message = "OK";
        String stationId = null;
        String body = null;
    }

    /**
     * Validates a PUT request for proper body and Lamport clock header.
     *
     * @param request the parsed {@link HTTPRequest}
     * @return a {@link PUTValidationResult} containing validation status, ID, body, and clock
     */
    private PUTValidationResult validatePUTRequest(HTTPRequest request) {
        PUTValidationResult result = new PUTValidationResult();

        String requestLamportClockString = request.headers.get("Lamport-Clock");
        if (requestLamportClockString == null) {
            result.status = 400;
            result.message = "Bad Request";
            return result;
        }

        try {
            result.requestLamportClock = Integer.parseInt(requestLamportClockString.trim());
        } catch (NumberFormatException e) {
            result.status = 400;
            result.message = "Bad Request";
            return result;
        }

        String body = request.body;
        if (body == null || !body.trim().startsWith("{")) {
            result.status = 204;
            result.message = "No Content";
            return result;
        }

        try {
            JsonObject json = new Gson().fromJson(body, JsonObject.class);
            if (json == null || !json.has("id")) {
                result.status = 400;
                result.message = "Bad Request";
                return result;
            }
            result.stationId = json.get("id").getAsString();
            result.body = body;
        } catch (JsonSyntaxException e) {
            result.status = 500;
            result.message = "Internal Server Error";
        }

        return result;
    }

    /**
     * Handles PUT requests.
     *
     * <p>Behavior:</p>
     * <ul>
     *   <li>Validates request body and Lamport clock.</li>
     *   <li>Inserts new station (201 Created) or updates existing station (200 OK).</li>
     *   <li>Persists the data into a JSON file via {@link FileHandler}.</li>
     *   <li>Increments server Lamport clock and update counter.</li>
     * </ul>
     *
     * @param request the parsed {@link HTTPRequest}
     * @return a serialized HTTP-like response string
     */
    private String handlePUTRequest(HTTPRequest request) {
        HTTPParser httpParser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();
        int curServerUpdateVal;

        PUTValidationResult putValidationResult = validatePUTRequest(request);

        synchronized (clockLock) {
            AggregationServer.ServerLamportClock =
                    Math.max(AggregationServer.ServerLamportClock, putValidationResult.requestLamportClock) + 1;
            AggregationServer.ServerUpdateCount++;
            curServerUpdateVal = AggregationServer.ServerUpdateCount;
            headers.put("Lamport-Clock", String.valueOf(AggregationServer.ServerLamportClock));
        }

        if (putValidationResult.status != 200) {
            return httpParser.createHTTPResponse(putValidationResult.status, putValidationResult.message, null, headers);
        }

        String stationId = putValidationResult.stationId;
        contentServerStationId = stationId;
        FileHandler handler = files.putIfAbsent(stationId, new FileHandler(stationId));

        if (handler == null) {
            putValidationResult.status = 201;
            putValidationResult.message = "Created";
            handler = files.get(stationId);
        }

        handler.writeToFile(putValidationResult.body, putValidationResult.requestLamportClock,
                System.currentTimeMillis(), curServerUpdateVal,
                socket.getInetAddress().getHostAddress(), socket.getPort());

        return httpParser.createHTTPResponse(putValidationResult.status, putValidationResult.message, null, headers);
    }

    /**
     * Handles SYNC requests for Lamport clock synchronization.
     *
     * <p>Behavior:</p>
     * <ul>
     *   <li>Updates server Lamport clock to {@code max(server, client) + 1}.</li>
     *   <li>Returns {@code 200 OK} with the new Lamport clock in headers.</li>
     * </ul>
     *
     * @param request the parsed {@link HTTPRequest}
     * @return a serialized HTTP-like response string
     */
    private String handleSYNCRequest(HTTPRequest request) {
        HTTPParser httpParser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();

        String requestLamportString = request.headers.get("Lamport-Clock");
        int requestLamportNumber = 0;
        if (requestLamportString != null) {
            try {
                requestLamportNumber = Integer.parseInt(requestLamportString);
            } catch (Exception ignored) {}
        }

        synchronized (clockLock) {
            AggregationServer.ServerLamportClock =
                    Math.max(AggregationServer.ServerLamportClock, requestLamportNumber) + 1;
            headers.put("Lamport-Clock", String.valueOf(AggregationServer.ServerLamportClock));
        }

        return httpParser.createHTTPResponse(200, "OK", null, headers);
    }
}


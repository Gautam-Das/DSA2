package AggregationServer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import shared.FileHandler;
import shared.HTTPParser;
import shared.HTTPRequest;
import shared.UrlManager;

import java.net.*;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AggregationServer {

    private ServerSocket serverSocket;
    private ConcurrentHashMap<String, FileHandler> files = new ConcurrentHashMap<>();
    public static int ServerLamportClock = 0;
    public static int ServerUpdateCount = 0;
    // Constructor with Port
    public AggregationServer(int port) {
        try {
            serverSocket = new ServerSocket(port);
            System.out.printf("Server connected to Socket on port %d\n", port);

        } catch (IOException i) {
            System.out.println(i.getMessage());
        }
    }

    public void run() {
        // garbage collection thread to delete files
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(120_000); // every 2 minutes
                    for (ConcurrentHashMap.Entry<String, FileHandler> entry : files.entrySet()){
                        String id = entry.getKey();
                        FileHandler handler = entry.getValue();
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

        while (true) {
            try {
                System.out.println("... waiting for connection");
                // start new thread on each client connection
                // Initialise sockets and stream
                Socket socket = serverSocket.accept(); // blocks this thread ?? till we receive a connection

                System.out.printf("client / server from port %d connected\n", socket.getPort());

                //TODO: implement a client pool rather than just threads
                new Thread(new handleConnection(socket, files)).start();
            } catch (IOException i) {
                System.out.println(i.getMessage());
            } catch (NullPointerException n) {
                System.out.println(n.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 2 || !args[0].equals("-p")) {
            System.err.println("Usage: java Main -p <port>");
            System.exit(1);
        }
        String portStr = args[1];
        int port = UrlManager.validateAndGetPort(portStr);
        if (port == -1) {
            System.exit(1);
        }
        AggregationServer aggregationServer = new AggregationServer(port);
        //TODO: set up existing files
        aggregationServer.run();
    }
}

class handleConnection implements Runnable {
    private Socket socket = null;
    private String message = "";
    private ConcurrentHashMap<String, FileHandler> files;
    handleConnection(Socket socket, ConcurrentHashMap<String, FileHandler> files) {
        this.socket = socket;
        this.files = files;
    }
    private static final Object clockLock = new Object();

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
                } catch (EOFException eof) {
                    System.out.printf("Client %d closed connection\n", socket.getPort());
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

    private static class GETValidationResult {
        int status = 200;
        String message = "OK";
        String id = null;
        int requestLamportClock = 0;
    }

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

        // parse lamport clock to int
        int requestLamportClock = 0;
        try {
            requestLamportClock = Integer.parseInt(requestLamportClockString.trim());
        } catch (NumberFormatException e) {
            result.status = 400;
            result.message = "Bad Request";
            return result;
        }


        String id = request.uri.replace("/", "");
        if (!id.isEmpty()) {
            result.id = id; // single resource
        }

        result.requestLamportClock = requestLamportClock;
        return result;
    }

    private String handleGETRequest(HTTPRequest request) {
        HTTPParser httpParser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();

        GETValidationResult getValidationResult = validateGETRequest(request);

        // update server lamport clock value
        synchronized (clockLock) {
            AggregationServer.ServerLamportClock = Math.max(AggregationServer.ServerLamportClock, getValidationResult.requestLamportClock) + 1;
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
                if (count > 0) {
                    bodyBuilder.append(",");
                }
                count++;
                bodyBuilder.append(handler.getSerializedObj());
            }

            bodyBuilder.append("]");

            return httpParser.createHTTPResponse(getValidationResult.status, getValidationResult.message, bodyBuilder.toString(), headers);

        } else {
            // return the one match id else bad request
            FileHandler fileHandler = files.get(getValidationResult.id);
            if (fileHandler == null || fileHandler.isExpired(AggregationServer.ServerUpdateCount)) {
                return httpParser.createHTTPResponse(400, "Bad Request", null, headers);
            } else {
                return httpParser.createHTTPResponse(200, "OK", fileHandler.getSerializedObj(), headers);
            }
        }
    }

    private static class PUTValidationResult {
        int status = 200;
        int requestLamportClock = 0;
        String message = "OK";
        String stationId = null;
        String body = null;
    }

    private PUTValidationResult validatePUTRequest(HTTPRequest request) {
        PUTValidationResult result = new PUTValidationResult();

        String requestLamportClockString = request.headers.get("Lamport-Clock");
        if (requestLamportClockString == null) {
            result.status = 400;
            result.message = "Bad Request";
            return result;
        }

        // parse lamport clock to int
        int requestLamportClock = 0;
        try {
            requestLamportClock = Integer.parseInt(requestLamportClockString.trim());
        } catch (NumberFormatException e) {
            result.status = 400;
            result.message = "Bad Request";
            return result;
        }

        // parse body
        String body = request.body;

        if (body == null || !body.trim().startsWith("{")) {
            result.status = 204;
            result.message = "No Content";
            return result;
        }

        Gson gson = new Gson();
        JsonObject json = null;
        try {
            json = gson.fromJson(body, JsonObject.class);
        } catch (JsonSyntaxException e) {
            result.status = 500;
            result.message = "Internal Server Error";
            return result;
        }

        // GET id
        if (json == null || !json.has("id")) {
            result.status = 400;
            result.message = "Bad Request";
            return result;
        }

        result.stationId = json.get("id").getAsString();
        result.requestLamportClock = requestLamportClock;
        result.body = body;
        return result;
    }

    private String handlePUTRequest(HTTPRequest request) {
        // Get lamport clock
        HTTPParser httpParser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();
        int curServerUpdateVal;
        PUTValidationResult putValidationResult = validatePUTRequest(request);

        // update server lamport clock value
        synchronized (clockLock) {
            AggregationServer.ServerLamportClock = Math.max(AggregationServer.ServerLamportClock, putValidationResult.requestLamportClock) + 1;
            AggregationServer.ServerUpdateCount++;
            curServerUpdateVal = AggregationServer.ServerUpdateCount;

            headers.put("Lamport-Clock", String.valueOf(AggregationServer.ServerLamportClock));
        }

        if (putValidationResult.status != 200) {
            return httpParser.createHTTPResponse(putValidationResult.status, putValidationResult.message, null, headers);
        }

        String stationId = putValidationResult.stationId;
        FileHandler handler = files.putIfAbsent(stationId, new FileHandler(stationId));

        // handler is null if stationId doesn't already exist
        if (handler == null) {
            // if newly created that status must be 201
            putValidationResult.status = 201;
            putValidationResult.message = "Created";
            handler  = files.get(stationId);
        }

        System.out.println("SAVING TO FILE:");
        System.out.println(putValidationResult.body);

        handler.writeToFile(putValidationResult.body, putValidationResult.requestLamportClock,
                System.currentTimeMillis(), curServerUpdateVal,
                socket.getInetAddress().getHostAddress(), socket.getPort());

        return httpParser.createHTTPResponse(putValidationResult.status, putValidationResult.message, null, headers);
    }

    private String handleSYNCRequest(HTTPRequest request) {
        HTTPParser httpParser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();

        synchronized (clockLock) {
            AggregationServer.ServerLamportClock++;
            headers.put("Lamport-Clock", String.valueOf(AggregationServer.ServerLamportClock));
        }

        return httpParser.createHTTPResponse(200, "OK", null, headers);
    }
}

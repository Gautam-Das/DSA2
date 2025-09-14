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
import java.util.concurrent.ConcurrentHashMap;

public class AggregationServer {

    private ServerSocket serverSocket;
    private ConcurrentHashMap<String, FileHandler> files = new ConcurrentHashMap<>();
    public static int ServerLamportClock = 0;
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
        try {
            DataInputStream inputStream = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream())
            );
            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
//            System.out.println("why isn't it responding");

            // get message from connection
            message = inputStream.readUTF();
            System.out.println(message);
            HTTPParser parser = new HTTPParser();
            HTTPRequest request = parser.parseHttpRequest(message);

//            System.out.println("request parsed successfully");

            String response = null;

            if (request == null) {
                System.err.println("Parsing failed on message");
            } else if (request.method.equals("GET")) {
                response = handleGETRequest(request);
            } else if (request.method.equals("PUT")) {
                response = handlePUTRequest(request);
            } else {
                System.err.println("Invalid HTTP request, must be either a GET or PUT request");
            }

            if (response == null) {
                HashMap<String, String> headers = new HashMap<>();
                synchronized (clockLock) {
                    AggregationServer.ServerLamportClock = AggregationServer.ServerLamportClock + 1;
                    headers.put("Lamport-Clock", String.valueOf(AggregationServer.ServerLamportClock));
                }
                HTTPParser httpParser = new HTTPParser();
                response = httpParser.createHTTPResponse(400, "Bad Request", null, headers);
            }

            System.out.printf("sending Response to socket %d:\n", socket.getPort());
            System.out.println(response);
            outputStream.writeUTF(response);

            // Close connection
            System.out.println("Closing connection");
            socket.close();
            inputStream.close();
        } catch (IOException i) {
            System.err.println("Error while creating response");
            System.err.println(i.getMessage());
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
            if (fileHandler == null) {
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

        PUTValidationResult putValidationResult = validatePUTRequest(request);

        // update server lamport clock value
        synchronized (clockLock) {
            AggregationServer.ServerLamportClock = Math.max(AggregationServer.ServerLamportClock, putValidationResult.requestLamportClock) + 1;
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

        handler.writeToFile(putValidationResult.body, putValidationResult.requestLamportClock);

        return httpParser.createHTTPResponse(putValidationResult.status, putValidationResult.message, null, headers);
    }
}

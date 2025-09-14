package GETclient;

import shared.*;

import java.net.*;
import java.io.*;
import java.util.HashMap;

public class GETClient {
    String aggServerIP;
    int aggServerPort;
    String stationID;
    int lamportClock = 0;

    private Socket socket;
    private DataOutputStream outputStream;
    private DataInputStream inputStream;

    // Constructor to put IP address and port
    public GETClient(String addr, int port, String stationId) {
        aggServerIP = addr;
        aggServerPort = port;
        this.stationID = stationId;
    }

    public void connectToServer() throws Exception {
        RetryUtils.withRetries(() -> {
            socket = new Socket(aggServerIP, aggServerPort);
            outputStream = new DataOutputStream(socket.getOutputStream());
            inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            return null;
        }, 5, 1000);
        System.out.printf("Connected to AggregationServer at %s:%d\n", aggServerIP, aggServerPort);
    }

    public void closeConnection() {
        try {
            if (outputStream != null) outputStream.close();
            if (inputStream != null) inputStream.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }
    public void syncWithServer() throws Exception {
        HTTPParser parser = new HTTPParser();

        RetryUtils.withRetries(() -> {
            HashMap<String, String> headers = new HashMap<>();
            lamportClock++;
            headers.put("Lamport-Clock", String.valueOf(lamportClock));

            String request = parser.createHTTPRequest("SYNC", "/", null, headers);
            System.out.println("Sending Lamport SYNC request:");
            outputStream.writeUTF(request);
            String response = inputStream.readUTF();

            HTTPResponse httpResponse = parser.parseHttpResponse(response);
            if (httpResponse == null) throw new IOException("Invalid response");
            if (httpResponse.statusCode == 500) throw new IOException("Server Error");
            // Bad request
            if (httpResponse.statusCode != 200) return null;
            String serverLamport = httpResponse.headers.get("Lamport-Clock");
            if (serverLamport == null) throw new IOException("Missing Lamport-Clock header");

            lamportClock = Math.max(lamportClock, Integer.parseInt(serverLamport)) + 1;
            System.out.printf("Synced successfully, new lamport clock: %d\n", lamportClock);
            return null;
        },5, 1000);
    }


    public String run() throws Exception {
        HTTPParser httpParser = new HTTPParser();

        return RetryUtils.withRetries(() -> {
            lamportClock++;
            HashMap<String, String> headers = new HashMap<>();
            headers.put("Lamport-Clock", String.valueOf(lamportClock));
            String message = httpParser.createHTTPRequest("GET",
                    (this.stationID == null ? "/" : "/" + this.stationID), null, headers);

            outputStream.writeUTF(message);

            String response = inputStream.readUTF();
            HTTPResponse httpResponse = httpParser.parseHttpResponse(response);
            if (httpResponse == null) throw new IOException("Invalid response");
            // only retry on 500
            if (httpResponse.statusCode == 500) throw new IOException("Server Error");
            // bad request
            if (httpResponse.statusCode != 200) return null;

            String serverLamport = httpResponse.headers.get("Lamport-Clock");
            if (serverLamport == null) throw new IOException("Missing Lamport-Clock header");

            lamportClock = Math.max(lamportClock, Integer.parseInt(serverLamport)) + 1;
            System.out.printf("Synced successfully, new lamport clock: %d\n", lamportClock);

            //TODO: PRETTY PRINT THE Response
            System.out.println("Parsed Response Status: " + httpResponse.statusCode + " " + httpResponse.statusMessage);
            System.out.println("Headers: " + httpResponse.headers);
            System.out.println("Body: " + httpResponse.body);

            return response;
        }, 5,1000);
    }

    public static void main(String[] args) {
        if (args.length < 2 || !args[0].equals("-url")) {
            System.err.println("Usage: java GETclient -url {server url} [-sid {station ID}]");
            System.exit(1);
        }

        String serverUrl = args[1];
        String stationId = null;

        // Optional -sid
        if (args.length == 4 && args[2].equals("-sid")) {
            stationId = args[3];
        } else if (args.length > 2) {
            System.err.println("Invalid arguments. Usage: java GETclient.GETClient -url {server url} [-sid {station ID}]");
            System.exit(1);
        }

        IpPort ipPort = UrlManager.getIPandPort(serverUrl);
        if (ipPort.port == -1){
            System.exit(1);
        }

        GETClient client = new GETClient(ipPort.ip, ipPort.port, stationId);
        try {
            client.connectToServer();
            client.syncWithServer();
            client.run();
            client.closeConnection();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}

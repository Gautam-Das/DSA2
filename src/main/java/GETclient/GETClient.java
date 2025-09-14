package GETclient;

import shared.*;

import java.net.*;
import java.io.*;
import java.util.HashMap;

/**
 * The {@code GETClient} connects to an {@link AggregationServer} and fetches stored
 * weather data entries using Lamport clock synchronization.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Connects to the AggregationServer over TCP.</li>
 *   <li>Synchronizes Lamport clocks with the server via SYNC requests.</li>
 *   <li>Sends GET requests to retrieve either all entries or a specific station ID.</li>
 *   <li>Handles retries on network errors and server failures.</li>
 * </ul>
 */
public class GETClient {
    String aggServerIP;
    int aggServerPort;
    String stationID;
    int lamportClock = 0;

    private Socket socket;
    private DataOutputStream outputStream;
    private DataInputStream inputStream;

    /**
     * Creates a new GETClient.
     *
     * @param addr      the IP address of the AggregationServer
     * @param port      the port number of the AggregationServer
     * @param stationId optional station ID to fetch; if {@code null}, fetches all stations
     */
    public GETClient(String addr, int port, String stationId) {
        aggServerIP = addr;
        aggServerPort = port;
        this.stationID = stationId;
    }

    /**
     * Establishes a TCP connection to the AggregationServer.
     *
     * <p>This method uses {@link RetryUtils#withRetries} to automatically retry
     * the connection up to 5 times with exponential backoff.</p>
     *
     * @throws Exception if the connection cannot be established after retries
     */
    public void connectToServer() throws Exception {
        RetryUtils.withRetries(() -> {
            socket = new Socket(aggServerIP, aggServerPort);
            outputStream = new DataOutputStream(socket.getOutputStream());
            inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            return null;
        }, 5, 1000);
        System.out.printf("Connected to AggregationServer at %s:%d\n", aggServerIP, aggServerPort);
    }

    /**
     * Closes the connection to the AggregationServer and cleans up resources.
     */
    public void closeConnection() {
        try {
            if (outputStream != null) outputStream.close();
            if (inputStream != null) inputStream.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }

    /**
     * Synchronizes this GETClient’s Lamport clock with the AggregationServer.
     *
     * <p>Sends a {@code SYNC} request with the local Lamport clock, receives the server’s
     * Lamport clock, and updates the local clock using {@code max(local, server) + 1}.</p>
     *
     * @throws Exception if synchronization fails after retries
     */
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
            // Bad request, don't retry
            if (httpResponse.statusCode >= 400) return null;

            String serverLamport = httpResponse.headers.get("Lamport-Clock");
            if (serverLamport == null) throw new IOException("Missing Lamport-Clock header");

            lamportClock = Math.max(lamportClock, Integer.parseInt(serverLamport)) + 1;
            System.out.printf("Synced successfully, new lamport clock: %d\n", lamportClock);
            return null;
        },5, 1000);
    }

    /**
     * Sends a GET request to the AggregationServer to fetch weather data.
     *
     * <p>If {@code stationID} is {@code null}, fetches all entries;
     * otherwise, fetches only the entry for the specified station.</p>
     *
     * <p>Lamport clock is incremented before each request and updated based on
     * the server’s response using {@code max(local, server) + 1}.</p>
     *
     * @return the raw HTTP response string from the AggregationServer
     * @throws Exception if the request fails after retries
     */
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
            // bad request, dont retry
            if (httpResponse.statusCode >= 400) return null;

            String serverLamport = httpResponse.headers.get("Lamport-Clock");
            if (serverLamport == null) throw new IOException("Missing Lamport-Clock header");

            lamportClock = Math.max(lamportClock, Integer.parseInt(serverLamport)) + 1;
            System.out.printf("Synced successfully, new lamport clock: %d\n", lamportClock);

            //TODO: Pretty print response for debugging
            System.out.println("Parsed Response Status: " + httpResponse.statusCode + " " + httpResponse.statusMessage);
            System.out.println("Headers: " + httpResponse.headers);
            System.out.println("Body: " + httpResponse.body);

            return response;
        }, 5,1000);
    }

    /**
     * Main entry point for running the GETClient from the command line.
     *
     * <p>Expected usage:</p>
     * <pre>
     * java GETclient.GETClient -url {server url} [-sid {station ID}]
     * </pre>
     *
     * @param args command-line arguments: {@code -url}, server URL, and optionally {@code -sid}, station ID
     */
    public static void main(String[] args) {
        if (args.length < 2 || !args[0].equals("-url")) {
            System.err.println("Usage: java GETclient -url {server url} [-sid {station ID}]");
            System.exit(1);
        }

        String serverUrl = args[1];
        String stationId = null;

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

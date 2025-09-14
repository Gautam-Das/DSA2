package ContentServer;

import shared.*;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;

/**
 * The {@code ContentServer} reads local weather data from a file and pushes it
 * to the {@link AggregationServer} using Lamport clock synchronization.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Connects to the AggregationServer over TCP.</li>
 *   <li>Synchronizes Lamport clocks with the server via SYNC requests.</li>
 *   <li>Parses a weather data file into entries and sends them via PUT requests.</li>
 *   <li>Handles retries on network errors and server failures.</li>
 * </ul>
 */
public class ContentServer {
    String aggServerIP;
    int aggServerPort;
    String filename;
    int lamportClock;

    private Socket socket;
    private DataOutputStream outputStream;
    private DataInputStream inputStream;

    /**
     * Creates a new ContentServer.
     *
     * @param aggServerIP  the IP address of the AggregationServer
     * @param aggServerPort the port number of the AggregationServer
     * @param filename     the path to the local weather data file
     */
    public ContentServer(String aggServerIP, int aggServerPort, String filename) {
        this.aggServerIP = aggServerIP;
        this.aggServerPort = aggServerPort;
        this.filename = filename;
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
     * Synchronizes this ContentServer’s Lamport clock with the AggregationServer.
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
            // don't retry on Bad requests
            if (httpResponse.statusCode >= 400) return null;

            String serverLamport = httpResponse.headers.get("Lamport-Clock");
            if (serverLamport == null) throw new IOException("Missing Lamport-Clock header");

            lamportClock = Math.max(lamportClock, Integer.parseInt(serverLamport)) + 1;
            System.out.printf("Synced successfully, new lamport clock: %d\n", lamportClock);
            return null;
        },5, 1000);
    }

    /**
     * Reads the local weather data file and sends each entry to the AggregationServer.
     *
     * <p>Entries are expected in {@code key:value} format, with each entry beginning
     * with an {@code id} field. Multiple entries can be contained in a single file.</p>
     *
     * <p>Special cases:</p>
     * <ul>
     *   <li>Entries without an {@code id} are skipped.</li>
     *   <li>Malformed lines are ignored.</li>
     * </ul>
     */
    public void run() {
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            HashMap<String, String> entry = new HashMap<>();

            while ((line = reader.readLine()) != null) {
                line = line.trim();

                // Split attribute:value
                String[] parts = line.split(":", 2);
                if (parts.length == 2) {
                    String key = parts[0].trim();
                    String value = parts[1].trim();

                    if (key.equals("id") && !entry.isEmpty()) {
                        processEntry(entry);
                        entry = new HashMap<>();
                    }
                    entry.put(key, value);
                }
            }

            // Process last entry if present
            if (!entry.isEmpty()) {
                processEntry(entry);
            }

        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
    }

    /**
     * Sends a single weather data entry to the AggregationServer as a PUT request.
     *
     * <p>Retries up to 5 times if the server responds with an error (500) or if
     * the network connection fails.</p>
     *
     * @param entry a map of key-value pairs representing the weather data entry
     */
    private void processEntry(HashMap<String, String> entry) {
        if (!entry.containsKey("id")) {
            System.err.println("Skipping entry without ID.");
            return;
        }

        try {
            RetryUtils.withRetries(() -> {
                // Increment Lamport clock before sending
                lamportClock++;

                HTTPParser parser = new HTTPParser();
                HashMap<String, String> headers = new HashMap<>();
                headers.put("Lamport-Clock", String.valueOf(lamportClock));

                String request = parser.createHTTPRequest("PUT", "/weather.json", entry, headers);

                System.out.println("Sending request:\n" + request);

                // Send request
                outputStream.writeUTF(request);

                // Read server response
                String response = inputStream.readUTF();
                System.out.println("Raw Response:\n" + response);

                HTTPResponse httpResponse = parser.parseHttpResponse(response);
                if (httpResponse == null) throw new IOException("Invalid response");
                if (httpResponse.statusCode == 500) throw new IOException("Server Error");

                // if not 200 or 201, must be 400 which is just bad request, don't retry
                if (httpResponse.statusCode != 200 && httpResponse.statusCode != 201) {
                    return null;
                }

                String serverLamport = httpResponse.headers.get("Lamport-Clock");
                if (serverLamport != null) {
                    lamportClock = Math.max(lamportClock, Integer.parseInt(serverLamport)) + 1;
                    System.out.printf("Lamport clock updated to %d\n", lamportClock);
                }

                System.out.println("Entry sent successfully: " + entry.get("id"));
                return null;
            }, 5, 1000);
        } catch (Exception e) {
            System.err.printf("Failed to send entry %s after retries: %s\n",
                    entry.get("id"), e.getMessage());
        }
    }

    /**
     * Main entry point for running the ContentServer from the command line.
     *
     * <p>Expected usage:</p>
     * <pre>
     * java ContentServer -url {server url} -f {local weather filename}
     * </pre>
     *
     * @param args command-line arguments: {@code -url}, server URL, {@code -f}, filename
     */
    public static void main(String[] args) {
        if (args.length < 4 || !args[0].equals("-url") || !args[2].equals("-f")) {
            System.err.println("Usage: java ContentServer -url {server url} -f {local weather filename}");
            System.exit(1);
        }

        String serverUrl = args[1];
        String filename = args[3];

        // Validate and parse server URL into IP and port
        IpPort ipPort = UrlManager.getIPandPort(serverUrl);
        if (ipPort.port == -1) {
            System.exit(1);
        }

        // Create and run ContentServer instance
        ContentServer server = new ContentServer(ipPort.ip, ipPort.port, filename);
        try {
            server.connectToServer();
            server.syncWithServer();
            server.run();
            while(true) {
                // keep alive (to simulate continuous ContentServer operation)
                // closes only when process is killed
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}

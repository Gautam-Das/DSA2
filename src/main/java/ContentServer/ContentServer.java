package ContentServer;

import shared.HTTPParser;
import shared.HTTPResponse;
import shared.IpPort;
import shared.UrlManager;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;

public class ContentServer {
    String aggServerIP;
    int aggServerPort;
    String filename;
    int lamportClock;

    public ContentServer(String aggServerIP, int aggServerPort, String filename) {
        this.aggServerIP = aggServerIP;
        this.aggServerPort = aggServerPort;
        this.filename = filename;
    }

    private Socket socket;
    private DataOutputStream outputStream;
    private DataInputStream inputStream;

    public void connectToServer() throws IOException {
        socket = new Socket(aggServerIP, aggServerPort);
        outputStream = new DataOutputStream(socket.getOutputStream());
        inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
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

    public void syncWithServer() throws IOException {
        HTTPParser parser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();
        lamportClock++;
        headers.put("Lamport-Clock", String.valueOf(lamportClock));

        // SYNC request
        String request = parser.createHTTPRequest("SYNC", "/", null, headers);
        System.out.println("Sending Lamport SYNC request:");
        System.out.println(request);

        outputStream.writeUTF(request);

        String response = inputStream.readUTF();
        System.out.println("SYNC response: " + response);
        HTTPResponse httpResponse = parser.parseHttpResponse(response);
        if (httpResponse == null) {
            System.err.println("Invalid response received");
            return;
        }

        //TODO:RETRY LOGIC ON BAD STATUS OR MISSING LAMPORT CLOCK
        try {
            lamportClock = Math.max(lamportClock, Integer.parseInt(httpResponse.headers.get("Lamport-Clock"))) + 1;
            System.out.printf("Synced successfully, new lamport clock: %d \n", lamportClock);
        } catch (Exception ignore) {}
    }

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

    private void processEntry(HashMap<String, String> entry) {
        if (!entry.containsKey("id")) {
            System.err.println("Skipping entry without ID.");
            return;
        }

        // Increment Lamport clock before sending
        lamportClock++;

        try {
            HTTPParser parser = new HTTPParser();
            HashMap<String, String> headers = new HashMap<>();
            headers.put("Lamport-Clock", String.valueOf(lamportClock));

            String request = parser.createHTTPRequest("PUT", "/weather.json", entry, headers);

            System.out.println("sending request:");
            System.out.println(request);

            // Send request
            outputStream.writeUTF(request);

            // Read server response
            String response = inputStream.readUTF();

            System.out.println(response);

            HTTPResponse httpResponse = parser.parseHttpResponse(response);

            //TODO: HANDLE RETRIES
            //TODO: UPDATE LAMPORT CLOCK

        } catch (IOException e) {
            System.err.println("Error sending entry: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length < 4 || !args[0].equals("-url") || !args[2].equals("-f")) {
            System.err.println("Usage: java ContentServer.ContentServer -url {server url} -f {local weather filename}");
            System.exit(1);
        }

        String serverUrl = args[1];
        String filename = args[3];

        // Validate and parse server URL into IP and port
        IpPort ipPort = UrlManager.getIPandPort(serverUrl);
        if (ipPort.port == -1) {
            System.exit(1);
        }

        // Create and run ContentServer.ContentServer instance
        ContentServer server = new ContentServer(ipPort.ip, ipPort.port, filename);
        try {
            server.connectToServer();
            server.syncWithServer();
            server.run();
            server.closeConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

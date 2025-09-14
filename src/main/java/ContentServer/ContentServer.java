package ContentServer;

import shared.HTTPParser;
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

        try (
                Socket socket = new Socket(aggServerIP, aggServerPort);
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             ) {

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
            // Basic acknowledgment check
            if (response.toString().contains("201") || response.toString().contains("200")) {
                System.out.println("Entry sent successfully: " + entry.get("id"));
            } else {
                System.err.println("Failed to send entry " + entry.get("id") + ": " + response);
            }

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
        server.run();
    }
}

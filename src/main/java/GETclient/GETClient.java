package GETclient;

import shared.HTTPParser;
import shared.IpPort;
import shared.UrlManager;

import java.net.*;
import java.io.*;
import java.util.HashMap;

public class GETClient {
    String aggServerIP;
    int aggServerPort;
    String stationID;
    int lamportClock = 0;

    // Constructor to put IP address and port
    public GETClient(String addr, int port, String stationId) {
        aggServerIP = addr;
        aggServerPort = port;
        this.stationID = stationId;
    }

    public void run() {
        // Establish a connection
        // Initialize socket and input/output streams
        Socket socket;
        DataOutputStream outputStream;
        System.out.printf("Attempting to server on %s port %d with station id %s \n", aggServerIP, aggServerPort, this.stationID);

        try {
            socket = new Socket(aggServerIP, aggServerPort);
            System.out.println("Successfully Connected");
            // Sends output to the socket
            outputStream = new DataOutputStream(socket.getOutputStream());
        }
        catch (IOException i) {
            System.out.println(i.getMessage());
            return;
        }

        // Increment Lamport clock before sending
        lamportClock++;

        // String to read message from input
        HTTPParser httpParser = new HTTPParser();
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Lamport-Clock", String.valueOf(lamportClock));
        String message = httpParser.createHTTPRequest("GET", (this.stationID == null ? "/" : "/" + this.stationID), null, headers);

        try {
            outputStream.writeUTF(message);
        }
        catch (IOException i) {
            System.out.println(i);
        }

        try {
            DataInputStream inputStream = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream())
            );
            String response = inputStream.readUTF();
            System.out.println(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Close the connection
        try {
            outputStream.close();
            socket.close();
        }
        catch (IOException i) {
            System.out.println(i);
        }
    }

    public static void main(String[] args) {
        if (args.length < 2 || !args[0].equals("-url")) {
            System.err.println("Usage: java GETclient.GETClient -url {server url} [-sid {station ID}]");
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
        client.run();
    }
}

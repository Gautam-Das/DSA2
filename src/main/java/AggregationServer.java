import java.net.*;
import java.io.*;

public class AggregationServer {

    private ServerSocket serverSocket;

    // Constructor with Port
    public AggregationServer(int port) {
        try {
            serverSocket = new ServerSocket(port);
            System.out.printf("Server connected to Socket on port %d", port);

        } catch (IOException i) {
            System.out.println(i.getMessage());
        }

        while (true) {
            try {

                System.out.println("... waiting for client");
                // start new thread on each client connection
                // Initialise sockets and stream
                Socket socket = serverSocket.accept(); // blocks  till we receive a connection

                System.out.printf("client from port %d connected\n", socket.getPort());

                new Thread(new handleGETclient(socket)).start();
            } catch (IOException i) {
                System.out.println(i.getMessage());
            } catch (NullPointerException n) {
                System.out.println(n.getMessage());
            }
        }
    }

    private static int validateAndGetPort(String portStr) {
        int port;
        try {
            port = Integer.parseInt(portStr);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Port must be between 1 and 65535.");
            }
            return port;
        } catch (NumberFormatException e) {
            System.err.println("Invalid port number: must be an integer.");
            System.exit(1);
            return -1;
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return -1;
        }
    }

    public static void main(String[] args) {
        if (args.length != 2 || !args[0].equals("-p")) {
            System.err.println("Usage: java Main -p <port>");
            System.exit(1);
        }
        String portStr = args[1];
        int port = validateAndGetPort(portStr);
        AggregationServer aggregationServer = new AggregationServer(port);
    }
}

class handleGETclient implements Runnable {
    private Socket socket = null;
    private String message = "";

    handleGETclient(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            DataInputStream inputStream = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream())
            );

            // Reads message from client until "Over" is sent
            while (!message.equals("Over")) {
                try {
                    message = inputStream.readUTF();
                    System.out.printf("from socket %d: %s\n", socket.getPort(), message);
                } catch (IOException i) {
                    System.out.println(i.getMessage());
                }
            }
            System.out.println("Closing connection");

            // Close connection
            socket.close();
            inputStream.close();
        } catch (IOException i) {
            System.out.println(i.getMessage());
        }

    }

}
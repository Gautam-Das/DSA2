import java.net.*;
import java.io.*;

public class GETClient {

    // Constructor to put IP address and port
    public GETClient(String addr, int port)
    {
        // Establish a connection
        // Initialize socket and input/output streams
        Socket socket;
        DataInputStream inputStream;
        DataOutputStream outputStream;
        try {
            socket = new Socket(addr, port);
            System.out.println("Connected");

            // Takes input from terminal
            inputStream = new DataInputStream(System.in);

            // Sends output to the socket
            outputStream = new DataOutputStream(socket.getOutputStream());
        }
        catch (IOException i) {
            System.out.println(i.getMessage());
            return;
        }

        // String to read message from input
        String message = "";

        // Keep reading until "Over" is input
        while (!message.equals("Over")) {
            try {
                message = inputStream.readLine();
                outputStream.writeUTF(message);
            }
            catch (IOException i) {
                System.out.println(i);
            }
        }

        // Close the connection
        try {
            inputStream.close();
            outputStream.close();
            socket.close();
        }
        catch (IOException i) {
            System.out.println(i);
        }
    }

    public static void main(String[] args) {
        GETClient client = new GETClient("127.0.0.1", 1000);
    }
}

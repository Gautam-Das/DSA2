package shared;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class UrlManager {
    public static int validateAndGetPort(String portStr) throws IllegalArgumentException {
        int port;
        try {
            port = Integer.parseInt(portStr);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Port must be between 1 and 65535.");
            }
            return port;
        } catch (NumberFormatException e) {
            System.err.println("Invalid port number: must be an integer.");
            return -1;
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return -1;
        }
    }

    public static IpPort getIPandPort(String serverUrl) {
        IpPort result = new IpPort();
        result.ip = "";
        result.port = -1;
        // remove first occurrence of http:// or https://
        // regex:
        // ^ only look at the start of the string
        // https?  match http with optional s
        // :// match literally ://
        serverUrl = serverUrl.replaceFirst("^https?://", "");

        // split host and port
        String[] parts = serverUrl.split(":");
        if (parts.length != 2) {
            System.err.println("Invalid server url, url must be in one of the forms: \n" +
                    "http://servername.domain.domain:portnumber \n" +
                    "http://servername:portnumber \n" +
                    "servername:portnumber \n"
            );
            return result;
        }


        // implement get ip from address
        String host = parts[0];
        try {
            InetAddress addr = InetAddress.getByName(host);
            result.ip = addr.getHostAddress();
        } catch (UnknownHostException e) {
            System.err.println(e.getMessage());
            return result;
        }
        result.port = UrlManager.validateAndGetPort(parts[1]);

        return result;
    }
}

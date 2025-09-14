package shared;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 *  class for validating ports and parsing server URLs into
 * {@link IpPort} objects containing an IP address and port number.
 *
 * <p>This class provides two main functionalities:</p>
 * <ul>
 *   <li>Validating that a port string represents a valid port number
 *       (between 1 and 65535).</li>
 *   <li>Extracting and resolving the host and port from a server URL,
 *       converting hostnames into IP addresses.</li>
 * </ul>
 *
 * <p>Example valid URLs:</p>
 * <pre>
 * http://example.com:8080
 * https://myserver.local:9090
 * servername:1234
 * </pre>
 */
public class UrlManager {

    /**
     * Validates a string as a port number and returns it as an integer.
     *
     * <p>The port must be between 1 and 65535. If the string is not a valid
     * integer or outside the allowed range, -1 is returned and an error
     * message is logged to {@code System.err}.</p>
     *
     * @param portStr the port string to validate
     * @return the port number if valid, or -1 if invalid
     * @throws IllegalArgumentException if the port is parsed but outside
     *                                  the range (1–65535)
     */
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

    /**
     * Parses a server URL and resolves it into an {@link IpPort} object.
     *
     * <p>The URL must be in one of the following forms:</p>
     * <pre>
     * http://servername.domain.domain:portnumber
     * https://servername:portnumber
     * servername:portnumber
     * </pre>
     *
     * <p>The host portion is resolved into an IP address. If
     * resolution fails, the returned {@link IpPort} will contain an empty
     * IP string and a port of -1.</p>
     *
     * @param serverUrl the server URL to parse
     * @return an {@link IpPort} containing the resolved IP and port, or
     *         a result with {@code ip=""} and {@code port=-1} if invalid
     */
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

        // resolve host to IP
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

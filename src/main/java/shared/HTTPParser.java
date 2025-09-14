package shared;
import com.google.gson.Gson;
import java.util.HashMap;

public class HTTPParser {
    private static final String HTTP_VERSION = "HTTP/1.1";
    private static final String USER_AGENT = "ATOMClient/1/0";
    private final Gson gson = new Gson();

    /**
     * Create an HTTP Message that can be written to output Stream of a Socket.
     * HTTP version is set to 1.1 by default.
     * User-agent is set to ATOMClient/1/0 by default.
     * The function adds the content length and content type header as well, if needed.
     *
     * @param method request method, at the moment restricted to GET and PUT
     * @param requestTarget target URI
     * @param data object whose attributes will be serialised to create the request body
     * @param headers additional headers to be included in the request headers
     * @return HTTP Request Message
     */
    public String createHTTPRequest(String method, String requestTarget, Object data, HashMap<String, String> headers){
        if (method == null || (!method.equals("GET") && !method.equals("PUT"))) {
            throw new IllegalArgumentException("Only GET and PUT methods are supported.");
        }

        boolean isPUT = method.equals("PUT");

        StringBuilder request = new StringBuilder();

        // Request line → METHOD SP request-target SP HTTP-version CRLF
        request.append(method).append(" ").append(requestTarget).append(" ").append(HTTP_VERSION).append("\r\n");

        // Default headers
        request.append("User-Agent: ").append(USER_AGENT).append("\r\n");
        // request.append("Host: localhost\r\n");

        // Custom headers
        if (headers != null) {
            for (HashMap.Entry<String, String> entry : headers.entrySet()) {
                String key = entry.getKey();
                // ignore already existing headers
                if (key.equals("Content-Type") || key.equals("Content-length") || key.equals("User-Agent")) continue;
                request.append(entry.getKey()).append(": ").append(entry.getValue()).append("\r\n");
            }
        }

        String body = "";
        if (data != null && isPUT) {
            body = gson.toJson(data);
            request.append("Content-Type: application/json\r\n");
            request.append("Content-Length: ").append(body.getBytes().length).append("\r\n");
        }

        // End headers
        request.append("\r\n");

        // Body if applicable
        if (!body.isEmpty()) {
            request.append(body);
        }

        return request.toString();
    }

        /**
         * Create an HTTP response message that can be written to output Stream of a Socket.
         * HTTP version is set to 1.1 by default.
         * User-agent is set to ATOMClient/1/0 by default
         * The function adds the content length and content type header as well, if needed.
         *
         * @param statusCode one of 201, 200, 400 and 500
         * @param status corresponding HTTP status message
         * @param data object whose attributes will be serialised to create the response body
         * @param headers additional headers to be included in the response headers
         * @return HTTP response message
         */
    public String createHTTPResponse(Integer statusCode, String status, String data, HashMap<String, String> headers){
        StringBuilder response = new StringBuilder();

        // Status line → HTTP-version SP status-code SP reason-phrase CRLF
        response.append(HTTP_VERSION).append(" ").append(statusCode).append(" ").append(status).append("\r\n");

        // Default headers
        response.append("User-Agent: ").append(USER_AGENT).append("\r\n");

        // Custom headers
        if (headers != null) {
            for (HashMap.Entry<String, String> entry : headers.entrySet()) {
                response.append(entry.getKey()).append(": ").append(entry.getValue()).append("\r\n");
            }
        }

        if (data != null) {
            response.append("Content-Type: application/json\r\n");
            response.append("Content-Length: ").append(data.getBytes().length).append("\r\n");
        }

        // End headers
        response.append("\r\n");

        // Body if applicable
        if (data != null) {
            response.append(data);
        }

        return response.toString();
    }

    /**
     * Parse an HTTP request string into its components: method, URI, version,
     * headers, and body.
     *
     * <p>For example:</p>
     *
     * <pre>
     * GET /api/users/1 HTTP/1.1
     * Host: example.com
     * Content-Type: application/json
     *
     * {"user":"alice"}
     * </pre>
     *
     * @param httpRequest the raw HTTP request string
     * @return a HTTPRequest object, or null if parsing fails
     */
    public HTTPRequest parseHttpRequest(String httpRequest) {
        if (httpRequest == null || httpRequest.isEmpty()) {
            return null;
        }

        String[] lines = httpRequest.split("\\r?\\n");
        if (lines.length == 0) {
            return null;
        }

        HTTPRequest result = new HTTPRequest();

        // Parse request line
        String[] parts = lines[0].split("\\s+");
        if (parts.length < 3) {
            return null;
        }
        result.method = parts[0];
        result.uri = parts[1];
        result.httpVersion = parts[2];

        // Parse headers
        int i = 1;
        for (; i < lines.length; i++) {
            String line = lines[i];
            if (line.trim().isEmpty()) {
                i++; // next line onwards is the body
                break;
            }
            String[] headerParts = line.split(":", 2);
            if (headerParts.length != 2) {
                return null;
            }
            result.headers.put(headerParts[0], headerParts[1]);
        }

        // Parse body
        if (i < lines.length) {
            StringBuilder bodyBuilder = new StringBuilder();
            for (; i < lines.length; i++) {
                bodyBuilder.append(lines[i]);
                if (i < lines.length - 1) {
                    bodyBuilder.append("\n");
                }
            }
            result.body = bodyBuilder.toString();
        } else {
            result.body = "";
        }

        return result;
    }

}

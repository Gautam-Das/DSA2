package shared;

import java.util.HashMap;

public class HTTPResponse {
    public String httpVersion;
    public int statusCode;
    public String statusMessage;
    public HashMap<String, String> headers = new HashMap<>();
    public String body;
}

package shared;
import java.util.HashMap;

public class HTTPRequest {
    public String method;
    public String uri;
    public String httpVersion;
    public HashMap<String, String> headers = new HashMap<>();
    public String body;
}

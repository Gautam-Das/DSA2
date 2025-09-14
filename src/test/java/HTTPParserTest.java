import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import shared.HTTPParser;
import shared.HTTPRequest;

import java.util.HashMap;

class HTTPParserTest {

    HTTPParser parser = new HTTPParser();
    Gson gson = new Gson();

    static class Item {
        String name;
        double price;
        Item(String name, double price) {
            this.name = name;
            this.price = price;
        }
    }

    @Test
    void createHTTPRequest_GET() {
        String request = parser.createHTTPRequest(
                "GET",
                "/api/items/123",
                null,
                new HashMap<>() {{
                    put("Accept", "application/json");
                }}
        );

        String expected =
                "GET /api/items/123 HTTP/1.1\r\n" +
                        "User-Agent: ATOMClient/1/0\r\n" +
                        "Accept: application/json\r\n" +
                        "\r\n";

        assertEquals(expected, request);
    }

    @Test
    void createHTTPRequest_PUT() {
        Item item = new Item("Widget", 9.99);
        String json = gson.toJson(item);

        String request = parser.createHTTPRequest(
                "PUT",
                "/api/items/456",
                item,
                new HashMap<>() {{
                    put("Content-Type", "application/json");
                }}
        );

        String expected =
                "PUT /api/items/456 HTTP/1.1\r\n" +
                        "User-Agent: ATOMClient/1/0\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Content-Length: " + json.length() + "\r\n" +
                        "\r\n" +
                        json;

        assertEquals(expected, request);
    }

    @Test
    void createHTTPResponse_200_OK() {
        Item item = new Item("Widget", 9.99);
        String json = gson.toJson(item);

        String response = parser.createHTTPResponse(
                200,
                "OK",
                item,
                new HashMap<>()
        );

        String expected =
                "HTTP/1.1 200 OK\r\n" +
                        "User-Agent: ATOMClient/1/0\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Content-Length: " + json.length() + "\r\n" +
                        "\r\n" +
                        json;

        assertEquals(expected, response);
    }

    @Test
    void createHTTPResponse_201_Created() {
        Item item = new Item("New Widget", 12.50);
        String json = gson.toJson(item);

        String response = parser.createHTTPResponse(
                201,
                "Created",
                item,
                new HashMap<>() {{
                    put("Location", "/api/items/456");
                }}
        );

        String expected =
                "HTTP/1.1 201 Created\r\n" +
                        "User-Agent: ATOMClient/1/0\r\n" +
                        "Location: /api/items/456\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Content-Length: " + json.length() + "\r\n" +
                        "\r\n" +
                        json;

        assertEquals(expected, response);
    }

    @Test
    void createHTTPResponse_400_BadRequest() {
        var error = new HashMap<String, String>() {{
            put("error", "Invalid input");
        }};
        String json = gson.toJson(error);

        String response = parser.createHTTPResponse(
                400,
                "Bad Request",
                error,
                new HashMap<>()
        );

        String expected =
                "HTTP/1.1 400 Bad Request\r\n" +
                        "User-Agent: ATOMClient/1/0\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Content-Length: " + json.length() + "\r\n" +
                        "\r\n" +
                        json;

        assertEquals(expected, response);
    }

    @Test
    void createHTTPResponse_500_InternalError() {
        var error = new HashMap<String, String>() {{
            put("error", "Something went wrong");
        }};
        String json = gson.toJson(error);

        String response = parser.createHTTPResponse(
                500,
                "Internal Server Error",
                error,
                new HashMap<>()
        );

        String expected =
                "HTTP/1.1 500 Internal Server Error\r\n" +
                        "User-Agent: ATOMClient/1/0\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Content-Length: " + json.length() + "\r\n" +
                        "\r\n" +
                        json;

        assertEquals(expected, response);
    }

    @Test
    void parseHttpRequest_validGETRequest() {
        String request =
                "GET /api/users/1 HTTP/1.1\r\n" +
                        "Host: example.com\r\n" +
                        "Accept: application/json\r\n" +
                        "\r\n";

        HTTPRequest parsed = parser.parseHttpRequest(request);

        assertNotNull(parsed);
        assertEquals("GET", parsed.method);
        assertEquals("/api/users/1", parsed.uri);
        assertEquals("HTTP/1.1", parsed.httpVersion);
        assertEquals("example.com", parsed.headers.get("Host").trim());
        assertEquals("application/json", parsed.headers.get("Accept").trim());
        assertEquals("", parsed.body);
    }

    @Test
    void parseHttpRequest_validPOSTWithBody() {
        String request =
                "POST /login HTTP/1.1\r\n" +
                        "Host: example.com\r\n" +
                        "Content-Type: application/json\r\n" +
                        "\r\n" +
                        "{\"user\":\"alice\"}";

        HTTPRequest parsed = parser.parseHttpRequest(request);

        assertNotNull(parsed);
        assertEquals("POST", parsed.method);
        assertEquals("/login", parsed.uri);
        assertEquals("HTTP/1.1", parsed.httpVersion);
        assertEquals("example.com", parsed.headers.get("Host").trim());
        assertEquals("application/json", parsed.headers.get("Content-Type").trim());
        assertEquals("{\"user\":\"alice\"}", parsed.body);
    }

    @Test
    void parseHttpRequest_validPOSTWithBodyMultipleLines() {
        String request =
                "POST /login HTTP/1.1\r\n" +
                        "Host: example.com\r\n" +
                        "Content-Type: application/json\r\n" +
                        "\r\n" +
                        "{\n" +
                        "\"user\":\"alice\"" +
                        "\n}";

        HTTPRequest parsed = parser.parseHttpRequest(request);

        assertNotNull(parsed);
        assertEquals("POST", parsed.method);
        assertEquals("/login", parsed.uri);
        assertEquals("HTTP/1.1", parsed.httpVersion);
        assertEquals("example.com", parsed.headers.get("Host").trim());
        assertEquals("application/json", parsed.headers.get("Content-Type").trim());
        assertEquals("{\n\"user\":\"alice\"\n}", parsed.body);
    }

    @Test
    void parseHttpRequest_extraSpacesInRequestLine() {
        String request =
                "PUT    /items/42    HTTP/1.0\r\n" +
                        "Host: test.com\r\n" +
                        "\r\n";

        HTTPRequest parsed = parser.parseHttpRequest(request);

        assertNotNull(parsed);
        assertEquals("PUT", parsed.method);
        assertEquals("/items/42", parsed.uri);
        assertEquals("HTTP/1.0", parsed.httpVersion);
        assertEquals("test.com", parsed.headers.get("Host").trim());
    }

    @Test
    void parseHttpRequest_noHeadersJustBody() {
        String request =
                "POST /submit HTTP/1.1\r\n" +
                        "\r\n" +
                        "data=123";

        HTTPRequest parsed = parser.parseHttpRequest(request);

        assertNotNull(parsed);
        assertEquals("POST", parsed.method);
        assertEquals("/submit", parsed.uri);
        assertEquals("HTTP/1.1", parsed.httpVersion);
        assertTrue(parsed.headers.isEmpty());
        assertEquals("data=123", parsed.body);
    }

    @Test
    void parseHttpRequest_invalidRequestLine_returnsNull() {
        String request = "INVALIDREQUEST\r\n";
        HTTPRequest parsed = parser.parseHttpRequest(request);
        assertNull(parsed);
    }

    @Test
    void parseHttpRequest_invalidHeader_returnsNull() {
        String request =
                "GET / HTTP/1.1\r\n" +
                        "BadHeaderWithoutColon\r\n" +
                        "\r\n";

        HTTPRequest parsed = parser.parseHttpRequest(request);
        assertNull(parsed);
    }

    @Test
    void parseHttpRequest_nullOrEmpty_returnsNull() {
        assertNull(parser.parseHttpRequest(null));
        assertNull(parser.parseHttpRequest(""));
    }
}

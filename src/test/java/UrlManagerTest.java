import org.junit.jupiter.api.Test;
import shared.IpPort;
import shared.UrlManager;

import static org.junit.jupiter.api.Assertions.*;

class UrlManagerTest {

    @Test
    void validPort_returnsPort() {
        int port = UrlManager.validateAndGetPort("8080");
        assertEquals(8080, port);
    }

    @Test
    void tooLowPort_returnsMinusOne() {
        int port = UrlManager.validateAndGetPort("0");
        assertEquals(-1, port);
    }

    @Test
    void tooHighPort_returnsMinusOne() {
        int port = UrlManager.validateAndGetPort("70000");
        assertEquals(-1, port);
    }

    @Test
    void nonNumericPort_returnsMinusOne() {
        int port = UrlManager.validateAndGetPort("abc");
        assertEquals(-1, port);
    }

    @Test
    void edgeCasePort1_returns1() {
        int port = UrlManager.validateAndGetPort("1");
        assertEquals(1, port);
    }

    @Test
    void edgeCasePort65535_returns65535() {
        int port = UrlManager.validateAndGetPort("65535");
        assertEquals(65535, port);
    }

    @Test
    void validLocalhostUrl_returnsIPAndPort() {
        IpPort result = UrlManager.getIPandPort("http://localhost:8080");
        assertEquals("127.0.0.1", result.ip);
        assertEquals(8080, result.port);
    }

    @Test
    void validIpAddressUrl_returnsIpAndPort() {
        IpPort result = UrlManager.getIPandPort("https://127.0.0.1:9090");
        assertEquals("127.0.0.1", result.ip);
        assertEquals(9090, result.port);
    }

    @Test
    void missingPort_returnsMinusOne() {
        IpPort result = UrlManager.getIPandPort("http://localhost");
        assertEquals(-1, result.port);
    }

    @Test
    void invalidHost_returnsMinusOnePort() {
        IpPort result = UrlManager.getIPandPort("notarealhost:1234");
        assertEquals(-1, result.port);
        assertEquals("", result.ip);
    }

    @Test
    void validHostFormat_noHttp_returnsMinusOnePort() {
        IpPort result = UrlManager.getIPandPort("localhost:1234");
        assertEquals("127.0.0.1", result.ip);
        assertEquals(1234, result.port);
    }

    @Test
    void invalidUrlFormat_noPort_returnsMinusOnePort() {
        IpPort result = UrlManager.getIPandPort("http://localhost");
        assertEquals("", result.ip);
        assertEquals(-1, result.port);
    }

    @Test
    void validHostFormat_oneDomain_returnsMinusOnePort() {
        IpPort result = UrlManager.getIPandPort("http://google.com:80");
        assertEquals(80, result.port);
    }

    @Test
    void validHostFormat_twoDomain_returnsMinusOnePort() {
        IpPort result = UrlManager.getIPandPort("http://mail.google.com:80");
        assertEquals(80, result.port);
    }

    @Test
    void invalidPort_returnsMinusOnePort() {
        IpPort result = UrlManager.getIPandPort("http://localhost:abc");
        assertEquals("127.0.0.1", result.ip);
        assertEquals(-1, result.port);
    }
}


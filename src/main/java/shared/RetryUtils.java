package shared;

public class RetryUtils {
    public static <T> T withRetries(Retryable<T> action, int maxAttempts, long baseDelayMs) throws Exception {
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return action.call();
            } catch (Exception e) {
                lastException = e;
                System.err.printf("Attempt %d failed: %s\n", attempt, e.getMessage());

                if (attempt == maxAttempts) break;

                // exponential backoff with jitter
                long delay = (long) (baseDelayMs * Math.pow(2, attempt - 1));
                delay += (long) (Math.random() * 100); // jitter
                Thread.sleep(delay);
            }
        }
        assert lastException != null;
        throw lastException;
    }
}
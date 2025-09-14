package shared;

/**
 * Utility class providing retry logic with exponential backoff and jitter.
 *
 * <p>This method repeatedly executes a {@link Retryable} action until it
 * either succeeds or the maximum number of attempts is reached. Between
 * attempts, it waits using exponential backoff with a small random jitter.</p>
 *
 * <p>Backoff calculation:</p>
 * <pre>
 * delay = baseDelayMs * 2^(attempt - 1) + random(0..100)
 * </pre>
 *
 * @param <T> the return type of the action
 */
public class RetryUtils {

    /**
     * Executes an action with retry logic.
     *
     * <p>If the action throws an exception, it is retried up to
     * {@code maxAttempts} times. After each failed attempt, the thread sleeps
     * for an exponentially increasing duration with random jitter.</p>
     *
     * <p>Example usage:</p>
     * <pre>
     * String result = RetryUtils.withRetries(() -> {
     *     // some network call that may fail
     *     return httpClient.get("http://example.com");
     * }, 5, 1000);
     * </pre>
     *
     * @param action       the retryable action to execute
     * @param maxAttempts  maximum number of attempts before failing
     * @param baseDelayMs  initial delay in milliseconds before retrying
     * @return the result of the action, if it succeeds
     * @throws Exception if all attempts fail, the last exception is rethrown
     */
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

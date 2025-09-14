package shared;

@FunctionalInterface
public interface Retryable<T> {
    T call() throws Exception;
}
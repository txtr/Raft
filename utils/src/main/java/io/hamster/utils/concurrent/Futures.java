package io.hamster.utils.concurrent;

import java.util.concurrent.CompletableFuture;

/**
 * Utilities for creating completed and exceptional futures.
 */
public final class Futures {

    /**
     * Creates a future that is synchronously completed exceptionally.
     *
     * @param t The future exception.
     * @return The exceptionally completed future.
     */
    public static <T> CompletableFuture<T> exceptionalFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }
}

package com.sushruth.kafka.eventfinder.exception;

public class ConnectionNotFoundException extends RuntimeException {
    public ConnectionNotFoundException() {
    }

    public ConnectionNotFoundException(String message) {
        super(message);
    }

    public ConnectionNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectionNotFoundException(Throwable cause) {
        super(cause);
    }

    public ConnectionNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

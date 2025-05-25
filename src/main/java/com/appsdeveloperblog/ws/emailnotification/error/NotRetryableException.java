package com.appsdeveloperblog.ws.emailnotification.error;

public class NotRetryableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public NotRetryableException(String message) {
        super(message);
    }

    public NotRetryableException(Throwable cause) {
        super( cause);
    }

}

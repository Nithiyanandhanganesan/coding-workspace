package com.wrs.datamanagement.exception;

/**
 * Created by bmangalam on 4/29/17.
 */
public class WRSDataException extends Exception {
    public WRSDataException() {
    }

    public WRSDataException(String message) {
        super(message);
    }

    public WRSDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public WRSDataException(Throwable cause) {
        super(cause);
    }

    public WRSDataException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

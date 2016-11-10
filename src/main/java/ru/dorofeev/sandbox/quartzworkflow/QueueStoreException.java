package ru.dorofeev.sandbox.quartzworkflow;

public class QueueStoreException extends Exception {

	public QueueStoreException() {
	}

	public QueueStoreException(String message) {
		super(message);
	}

	public QueueStoreException(String message, Throwable cause) {
		super(message, cause);
	}

	public QueueStoreException(Throwable cause) {
		super(cause);
	}

	public QueueStoreException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

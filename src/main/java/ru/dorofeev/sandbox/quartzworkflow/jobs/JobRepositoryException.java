package ru.dorofeev.sandbox.quartzworkflow.jobs;

@SuppressWarnings({"WeakerAccess", "unused"})
public class JobRepositoryException extends RuntimeException {

	public JobRepositoryException() {
	}

	public JobRepositoryException(String message) {
		super(message);
	}

	public JobRepositoryException(String message, Throwable cause) {
		super(message, cause);
	}

	public JobRepositoryException(Throwable cause) {
		super(cause);
	}

	public JobRepositoryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

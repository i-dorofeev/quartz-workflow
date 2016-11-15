package ru.dorofeev.sandbox.quartzworkflow.jobs;

@SuppressWarnings({"WeakerAccess", "unused"})
public class JobRepositoryException extends RuntimeException {

	JobRepositoryException() {
	}

	JobRepositoryException(String message) {
		super(message);
	}

	JobRepositoryException(String message, Throwable cause) {
		super(message, cause);
	}

	JobRepositoryException(Throwable cause) {
		super(cause);
	}

	JobRepositoryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

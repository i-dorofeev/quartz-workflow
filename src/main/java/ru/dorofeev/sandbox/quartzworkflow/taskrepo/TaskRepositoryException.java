package ru.dorofeev.sandbox.quartzworkflow.taskrepo;

@SuppressWarnings({"WeakerAccess", "unused"})
public class TaskRepositoryException extends RuntimeException {

	TaskRepositoryException() {
	}

	TaskRepositoryException(String message) {
		super(message);
	}

	TaskRepositoryException(String message, Throwable cause) {
		super(message, cause);
	}

	TaskRepositoryException(Throwable cause) {
		super(cause);
	}

	TaskRepositoryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

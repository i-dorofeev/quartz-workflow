package ru.dorofeev.sandbox.quartzworkflow.engine;

@SuppressWarnings({"unused", "WeakerAccess"})
public class EngineException extends RuntimeException {

	EngineException() {
	}

	EngineException(String message) {
		super(message);
	}

	EngineException(String message, Throwable cause) {
		super(message, cause);
	}

	EngineException(Throwable cause) {
		super(cause);
	}

	EngineException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

package ru.dorofeev.sandbox.quartzworkflow.engine;

@SuppressWarnings({"unused", "WeakerAccess"})
public class EngineException extends RuntimeException {

	public EngineException() {
	}

	public EngineException(String message) {
		super(message);
	}

	public EngineException(String message, Throwable cause) {
		super(message, cause);
	}

	public EngineException(Throwable cause) {
		super(cause);
	}

	public EngineException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

package ru.dorofeev.sandbox.quartzworkflow.queue;

@SuppressWarnings("WeakerAccess")
public class QueueManagerException extends RuntimeException {

	QueueManagerException(String msg) {
		super(msg);
	}

	QueueManagerException(String message, Exception e) {
		super(message, e);
	}

	QueueManagerException(Exception e) {
		super(e);
	}
}

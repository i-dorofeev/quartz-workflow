package ru.dorofeev.sandbox.quartzworkflow.queue;

public class QueueManagerFactory {

	public static QueueManager create(String name, QueueStore store) {
		return new QueueManagerImpl(name, store);
	}
}

package ru.dorofeev.sandbox.quartzworkflow.queue;

public class QueueStoreFactory {

	public static QueueStore inMemoryQueueStore() {
		return new InMemoryQueueStore();
	}
}

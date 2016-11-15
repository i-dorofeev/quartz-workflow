package ru.dorofeev.sandbox.quartzworkflow.queue;

public class QueueStoreFactory {

	public static QueueStore createInMemoryStore() {
		return new QueueInMemoryStore();
	}

	public static QueueStore createSqlStore(String dataSourceUrl) {
		return new QueueSqlStore(dataSourceUrl);
	}
}

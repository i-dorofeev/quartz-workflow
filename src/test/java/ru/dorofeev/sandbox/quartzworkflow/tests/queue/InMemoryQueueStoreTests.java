package ru.dorofeev.sandbox.quartzworkflow.tests.queue;

import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStore;

import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueStoreFactory.inMemoryQueueStore;

public class InMemoryQueueStoreTests extends AbstractQueueStoreTest {

	private static final QueueStore queueStore = inMemoryQueueStore();

	@Override
	protected QueueStore queueStore() {
		return queueStore;
	}
}

package ru.dorofeev.sandbox.quartzworkflow.tests.jobstore;

import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStore;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;

import static ru.dorofeev.sandbox.quartzworkflow.jobs.JobStoreFactory.inMemoryJobStore;
import static ru.dorofeev.sandbox.quartzworkflow.serialization.SerializationFactory.jsonSerialization;

public class InMemoryJobStoreTest extends AbstractJobStoreTest {

	private static final SerializedObjectFactory serialization = jsonSerialization();
	private static final JobStore store = inMemoryJobStore().spawn(serialization);

	@Override
	protected JobStore getStore() {
		return store;
	}

	@Override
	protected SerializedObjectFactory getSerializedObjectFactory() {
		return serialization;
	}
}

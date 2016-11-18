package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import rx.functions.Func1;

public class JobStoreFactory {

	public static Func1<SerializedObjectFactory, JobStore> inMemoryJobStore() {
		return InMemoryJobStore::new;
	}
}

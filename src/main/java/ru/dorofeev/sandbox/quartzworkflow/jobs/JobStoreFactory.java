package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.jobs.ram.InMemoryJobStore;
import ru.dorofeev.sandbox.quartzworkflow.jobs.sql.SqlJobStore;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import ru.dorofeev.sandbox.quartzworkflow.utils.RandomUUIDGenerator;
import ru.dorofeev.sandbox.quartzworkflow.utils.UUIDGenerator;
import rx.functions.Func1;

import javax.sql.DataSource;

public class JobStoreFactory {

	public static Func1<SerializedObjectFactory, JobStore> inMemoryJobStore() {
		return InMemoryJobStore::new;
	}

	public static Func1<SerializedObjectFactory, JobStore> sqlJobStore(DataSource dataSource) {
		return sqlJobStore(dataSource, new RandomUUIDGenerator());
	}

	public static Func1<SerializedObjectFactory, JobStore> sqlJobStore(DataSource dataSource, UUIDGenerator uuidGenerator) {
		return soFactory -> {
			SqlJobStore sqlJobStore = new SqlJobStore(dataSource, uuidGenerator, soFactory);
			sqlJobStore.initialize();
			return sqlJobStore;
		};
	}
}

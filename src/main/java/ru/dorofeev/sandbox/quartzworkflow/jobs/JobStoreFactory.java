package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.jobs.sql.SqlJobStore;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import rx.functions.Func1;

import javax.sql.DataSource;

public class JobStoreFactory {

	public static Func1<SerializedObjectFactory, JobStore> inMemoryJobStore() {
		return InMemoryJobStore::new;
	}

	public static Func1<SerializedObjectFactory, JobStore> sqlJobStore(DataSource dataSource) {
		return soFactory -> {
			SqlJobStore sqlJobStore = new SqlJobStore(dataSource, soFactory);
			sqlJobStore.initialize();
			return sqlJobStore;
		};
	}
}

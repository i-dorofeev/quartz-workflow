package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.jobs.ram.InMemoryJobStore;
import ru.dorofeev.sandbox.quartzworkflow.jobs.sql.SqlJobStore;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import ru.dorofeev.sandbox.quartzworkflow.utils.RandomUUIDGenerator;
import ru.dorofeev.sandbox.quartzworkflow.utils.UUIDGenerator;

import javax.sql.DataSource;

@FunctionalInterface
public interface JobStoreFactory {

	JobStore spawn(SerializedObjectFactory serializedObjectFactory);

	static JobStoreFactory inMemoryJobStore() {
		return InMemoryJobStore::new;
	}

	static JobStoreFactory sqlJobStore(DataSource dataSource) {
		return sqlJobStore(dataSource, new RandomUUIDGenerator());
	}

	static JobStoreFactory sqlJobStore(DataSource dataSource, UUIDGenerator uuidGenerator) {
		return soFactory -> {
			SqlJobStore sqlJobStore = new SqlJobStore(dataSource, uuidGenerator, soFactory);
			sqlJobStore.initialize();
			return sqlJobStore;
		};
	}
}

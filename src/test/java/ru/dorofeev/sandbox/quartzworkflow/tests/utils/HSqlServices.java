package ru.dorofeev.sandbox.quartzworkflow.tests.utils;

import org.hibernate.dialect.HSQLDialect;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStore;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStoreFactory;
import ru.dorofeev.sandbox.quartzworkflow.queue.sql.SqlQueueStore;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import rx.functions.Func1;

import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueStoreFactory.sqlQueueStore;

public class HSqlServices {

	private final HSqlDb HSqlDb;

	public HSqlServices() {
		this.HSqlDb = new HSqlDb();
	}

	public Func1<SerializedObjectFactory, JobStore> jobStoreFactory() {
		return JobStoreFactory.sqlJobStore(HSqlDb.getDataSource());
	}

	public SqlQueueStore queueStore() {
		return sqlQueueStore(HSqlDb.getDataSource(), HSQLDialect.class, "/queueHibernateTests.cfg.xml", 10);
	}

	public void shutdown() {
		this.HSqlDb.shutdown();
	}
}

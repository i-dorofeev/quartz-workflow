package ru.dorofeev.sandbox.quartzworkflow.tests.queue;

import net.ttddyy.dsproxy.support.ProxyDataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.hibernate.dialect.HSQLDialect;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.sql.SqlQueueStore;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.HSqlDb;

public class SqlQueueStoreTests extends AbstractQueueStoreTest {

	private static HSqlDb hsql;
	private static SqlQueueStore queueStore;

	@Override
	protected QueueStore queueStore() {
		return queueStore;
	}

	@BeforeClass
	public static void beforeClass() {
		hsql = new HSqlDb("queueHibernateTests");

		ProxyDataSource hsqlDataSource = ProxyDataSourceBuilder
			.create(hsql.getDataSource())
			.logQueryToSysOut()
			.build();

		queueStore = new SqlQueueStore(hsqlDataSource, HSQLDialect.class, "/queueHibernateTests.cfg.xml", 5);
	}

	@AfterClass
	public static void afterClass() {
		hsql.shutdown();
	}
}

package ru.dorofeev.sandbox.quartzworkflow.tests.queue;

import net.ttddyy.dsproxy.support.ProxyDataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.hibernate.dialect.HSQLDialect;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.queue.sql.SqlQueueItem;
import ru.dorofeev.sandbox.quartzworkflow.queue.sql.SqlQueueStore;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.HSqlDb;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.PARALLEL;
import static rx.Observable.range;

public class SqlQueueStoreParallelExecutionTests {

	private static HSqlDb hsql;
	private static SqlQueueStore queueStore;


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

	@Test
	public void test050_parallelExecution() {

		range(0, 4)
			.forEach(i -> queueStore.insertQueueItem(new JobId("job" + i), "default", PARALLEL));

		SqlQueueStore.PopNextOperation op1 = queueStore.newPopNextOperation();
		op1.query("default", 5);

		SqlQueueStore.PopNextOperation op2 = queueStore.newPopNextOperation();
		op2.query("default", 5);

		List<SqlQueueItem> queueItems1 = op1.getQueueItems();
		List<SqlQueueItem> queueItems2 = op2.getQueueItems();

		op1.close();
		op2.close();

		assertThat(queueItems1, hasSize(4));
		assertThat(queueItems2, is(empty()));
	}
}

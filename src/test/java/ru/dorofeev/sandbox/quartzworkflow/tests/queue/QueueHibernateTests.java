package ru.dorofeev.sandbox.quartzworkflow.tests.queue;

import net.ttddyy.dsproxy.support.ProxyDataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.hibernate.dialect.HSQLDialect;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueItem;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType;
import ru.dorofeev.sandbox.quartzworkflow.queue.sql.SqlQueueItem;
import ru.dorofeev.sandbox.quartzworkflow.queue.sql.SqlQueueStore;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.HSqlDb;
import ru.dorofeev.sandbox.quartzworkflow.utils.UUIDGenerator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.PARALLEL;
import static rx.Observable.from;

@FixMethodOrder(NAME_ASCENDING)
public class QueueHibernateTests {

	private static final UUIDGenerator uuidGenerator = new SimpleUUIDGenerator();

	private static HSqlDb hsql;
	private static SqlQueueStore queueStore;

	private static final QueueTracker queueTracker = new QueueTracker();

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
	public void test010_initialData() {
		from(new ExecutionType[] {
					PARALLEL,
					PARALLEL,
					PARALLEL,
					PARALLEL,
					EXCLUSIVE,
					EXCLUSIVE,
					PARALLEL,
					PARALLEL,
					PARALLEL })
			.map(et -> queueStore.insertQueueItem(new JobId(uuidGenerator.newUuid()), null, et))
			.forEach(queueTracker::add);
	}

	@Test
	public void test020_popNext() {

		popNext(asList(1L, 2L, 3L, 4L));
		popNext(emptyList());
	}

	@Test
	public void test030_removeCompleted() {

		queueStore.removeQueueItem(queueTracker.byOrdinal(1L).getJobId());
		queueStore.removeQueueItem(queueTracker.byOrdinal(2L).getJobId());
		queueStore.removeQueueItem(queueTracker.byOrdinal(3L).getJobId());
		queueStore.removeQueueItem(queueTracker.byOrdinal(4L).getJobId());

		popNext(singletonList(5L));

		queueStore.removeQueueItem(queueTracker.byOrdinal(5L).getJobId());

		popNext(singletonList(6L));

		queueStore.removeQueueItem(queueTracker.byOrdinal(6L).getJobId());

		popNext(asList(7L, 8L, 9L));
	}

	@Test
	public void test040_parallelExecution() {

		from(new ExecutionType[] {
			PARALLEL,
			PARALLEL,
			PARALLEL,
			PARALLEL })
			.forEach(et -> queueStore.insertQueueItem(new JobId(uuidGenerator.newUuid()), null, et));

		SqlQueueStore.PopNextOperation op1 = queueStore.newPopNextOperation();
		op1.query(5);

		SqlQueueStore.PopNextOperation op2 = queueStore.newPopNextOperation();
		op2.query(5);

		List<SqlQueueItem> queueItems1 = op1.getQueueItems();
		List<SqlQueueItem> queueItems2 = op2.getQueueItems();

		op1.close();
		op2.close();

		assertThat(queueItems1, hasSize(4));
		assertThat(queueItems2, is(empty()));
	}

	private void popNext(List<Long> expectedItems) {
		System.out.println("Getting next " + expectedItems.size() + " queue items...");
		for (int i = 0; i < expectedItems.size(); i++) {
			Optional<JobId> jobIdOptional = queueStore.popNextPendingQueueItem(null);
			System.out.println(i + ": " + jobIdOptional);

			JobId jobId = jobIdOptional.orElseThrow(() -> new AssertionError("Unexpected empty jobId"));

			assertThat(queueTracker.byJobId(jobId).getOrdinal(), is(equalTo(expectedItems.get(i))));
		}
	}

	private static class SimpleUUIDGenerator implements UUIDGenerator {

		private int counter;

		@Override
		public String newUuid() {
			return Integer.toString(++counter);
		}
	}

	private static class QueueTracker {

		private final Map<JobId, QueueItem> itemsByJobId = new HashMap<>();
		private final Map<Long, QueueItem> itemsByOrdinal = new HashMap<>();

		void add(QueueItem qi) {
			itemsByJobId.put(qi.getJobId(), qi);
			itemsByOrdinal.put(qi.getOrdinal(), qi);
		}

		QueueItem byJobId(JobId jobId) {
			return itemsByJobId.get(jobId);
		}

		QueueItem byOrdinal(Long ordinal) {
			return itemsByOrdinal.get(ordinal);
		}

	}

}

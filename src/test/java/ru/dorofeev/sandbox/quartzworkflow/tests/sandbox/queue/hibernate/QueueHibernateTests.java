package ru.dorofeev.sandbox.quartzworkflow.tests.sandbox.queue.hibernate;

import net.ttddyy.dsproxy.support.ProxyDataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.hibernate.dialect.HSQLDialect;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.HSqlDb;
import ru.dorofeev.sandbox.quartzworkflow.utils.UUIDGenerator;
import rx.Observable;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.PARALLEL;
import static ru.dorofeev.sandbox.quartzworkflow.tests.sandbox.queue.hibernate.QueueItemStatus.PENDING;

@FixMethodOrder(NAME_ASCENDING)
public class QueueHibernateTests {

	private static final UUIDGenerator uuidGenerator = new SimpleUUIDGenerator();

	private static HSqlDb hsql;
	private static SqlQueueStore queueStore;

	@BeforeClass
	public static void beforeClass() {
		hsql = new HSqlDb("queueHibernateTests");

		ProxyDataSource hsqlDataSource = ProxyDataSourceBuilder
			.create(hsql.getDataSource())
			.logQueryToSysOut()
			.build();

		queueStore = new SqlQueueStore(hsqlDataSource, HSQLDialect.class, "/queueHibernateTests.cfg.xml");
	}

	@AfterClass
	public static void afterClass() {
		hsql.shutdown();
	}

	@Test
	public void test010_initialData() {
		List<QueueItem> queueItems = asList(
			new QueueItem(uuidGenerator.newUuid(), PARALLEL, PENDING),
			new QueueItem(uuidGenerator.newUuid(), PARALLEL, PENDING),
			new QueueItem(uuidGenerator.newUuid(), PARALLEL, PENDING),
			new QueueItem(uuidGenerator.newUuid(), PARALLEL, PENDING),
			new QueueItem(uuidGenerator.newUuid(), EXCLUSIVE, PENDING),
			new QueueItem(uuidGenerator.newUuid(), EXCLUSIVE, PENDING),
			new QueueItem(uuidGenerator.newUuid(), PARALLEL, PENDING),
			new QueueItem(uuidGenerator.newUuid(), PARALLEL, PENDING),
			new QueueItem(uuidGenerator.newUuid(), PARALLEL, PENDING));

		queueStore.enqueueItems(queueItems);
	}

	@Test
	public void test020_popNext() {

		popNext(5, asList(1, 2, 3, 4));
		popNext(5, emptyList());
	}

	@Test
	public void test030_removeCompleted() {

		queueStore.remove(1);
		queueStore.remove(2);
		queueStore.remove(3);
		queueStore.remove(4);

		popNext(5, singletonList(5));

		queueStore.remove(5);

		popNext(5, singletonList(6));

		queueStore.remove(6);

		popNext(5, asList(7, 8, 9));
	}

	@Test
	public void test040_parallelExecution() {

		List<QueueItem> queueItems = asList(
			new QueueItem(uuidGenerator.newUuid(), PARALLEL, PENDING),
			new QueueItem(uuidGenerator.newUuid(), PARALLEL, PENDING),
			new QueueItem(uuidGenerator.newUuid(), PARALLEL, PENDING),
			new QueueItem(uuidGenerator.newUuid(), PARALLEL, PENDING));

		queueStore.enqueueItems(queueItems);

		TransactionScope tx1 = new TransactionScope(queueStore.getSessionFactory());
		Observable<QueueItem> nextTx1 = queueStore.popNext(tx1, 5);

		List<QueueItem> qiTx1 = nextTx1.take(2).toList().toBlocking().single();
		System.out.println("qiTx1 = " + qiTx1);

		TransactionScope tx2 = new TransactionScope(queueStore.getSessionFactory());
		Observable<QueueItem> nextTx2 = queueStore.popNext(tx2, 5);

		List<QueueItem> qiTx2 = nextTx2.toList().toBlocking().single();
		System.out.println("q1Tx2 = " + qiTx2);

		tx1.transaction.commit();
		tx1.close();

		tx2.transaction.commit();
		tx2.close();
	}

	private void popNext(int maxResults, List<Integer> expectedItems) {
		List<QueueItem> queueItems = queueStore.popNext(maxResults);
		System.out.println(queueItems);

		assertThat(queueItems.stream().map(QueueItem::getOrdinal).collect(toList()), is(equalTo(expectedItems)));
	}

	private static class SimpleUUIDGenerator implements UUIDGenerator {

		private int counter;

		@Override
		public String newUuid() {
			return Integer.toString(++counter);
		}
	}
}

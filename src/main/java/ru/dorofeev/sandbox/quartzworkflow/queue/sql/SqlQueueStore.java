package ru.dorofeev.sandbox.quartzworkflow.queue.sql;

import org.hibernate.SessionFactory;
import org.hibernate.StaleStateException;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueItem;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueItemStatus;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStoreException;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType;
import rx.Observable;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.Collections.emptyList;
import static java.util.Optional.*;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.PARALLEL;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueItemStatus.PENDING;

@SuppressWarnings("JpaQlInspection")
public class SqlQueueStore implements QueueStore {

	private final SessionFactory sessionFactory;
	private final Queue<JobId> localQueue = new ConcurrentLinkedQueue<>();

	private final int fetchSize;

	public SqlQueueStore(DataSource dataSource, Class<? extends Dialect> dialect, String extraHibernateCfg, int fetchSize) {
		this.fetchSize = fetchSize;

		Configuration configuration = new Configuration()
			.addProperties(buildProperties(dataSource, dialect))
			.addAnnotatedClass(SqlQueueItem.class)
			.configure("/sqlQueueStore/sqlQueueStore.cfg.xml");

		if (extraHibernateCfg != null)
			configuration.configure(extraHibernateCfg);

		//noinspection deprecation
		this.sessionFactory = configuration.buildSessionFactory();
	}

	private Properties buildProperties(DataSource ds, Class<? extends Dialect> dialect) {
		Properties properties = new Properties();
		properties.put("hibernate.connection.datasource", ds);
		properties.put("hibernate.dialect", dialect.getName());
		return properties;
	}

	private List<SqlQueueItem> popNext(int maxResults) {

		try (PopNextOperation op = newPopNextOperation()) {

			op.query(maxResults);
			return op.getQueueItems();
		}
	}

	// "public" for testing purpose
	public PopNextOperation newPopNextOperation() {
		return new PopNextOperation();
	}

	@Override
	public QueueItem insertQueueItem(JobId jobId, String queueName, ExecutionType executionType) throws QueueStoreException {

		try (TransactionScope tx = new TransactionScope(sessionFactory)) {

			SqlQueueItem queueItem = new SqlQueueItem(jobId.toString(), executionType, PENDING);
			tx.session.save(queueItem);

			tx.transaction.commit();

			return queueItem;
		}
	}

	@Override
	public Optional<JobId> popNextPendingQueueItem(String queueName) {

		synchronized (localQueue) {
			JobId nextJobId = localQueue.poll();
			if (nextJobId != null)
				return of(nextJobId);

			popNext(fetchSize).stream()
				.map(QueueItem::getJobId)
				.forEach(localQueue::add);

			return ofNullable(localQueue.poll());
		}
	}

	@Override
	public Optional<String> removeQueueItem(JobId jobId) {
		try (TransactionScope tx = new TransactionScope(sessionFactory)) {

			tx.session
				.createQuery("delete SqlQueueItem where jobId=:jobId")
				.setParameter("jobId", jobId.toString())
				.executeUpdate();

			tx.transaction.commit();

			return empty();
		}
	}

	// PopNext operation is extracted into a seperate class in order to test a scenario
	// of a simultaneous claiming of queue items
	public class PopNextOperation implements AutoCloseable {

		private final TransactionScope tx;

		private List<SqlQueueItem> results;

		PopNextOperation() {
			this.tx = new TransactionScope(sessionFactory);
		}

		public void query(int maxResults) {
			results = popNext(tx, maxResults)
				.toList().toBlocking().single();
		}

		public List<SqlQueueItem> getQueueItems() {
			if (results == null)
				throw new IllegalStateException("Invoke query() first.");

			try {
				this.tx.transaction.commit();
				return results;
			} catch (StaleStateException e) {
				// someone's already acquired queue items in a parallel transaction
				// (implemented using hibernate optimistic locking feature (SqlQueueItem.version column))
				return emptyList();
			}
		}

		private Observable<SqlQueueItem> popNext(TransactionScope tx, int maxResults) {

			return Observable.<SqlQueueItem>create(subscriber -> {
				List<SqlQueueItem> nextItems = fetchNextPendingQueueItems(tx, maxResults);
				System.out.println(nextItems);

				ExecutionType executionType = fetchCurrentExecutionType(tx);
				System.out.println("Current execution type = " + executionType);

				for (SqlQueueItem qi: nextItems) {
					if (PARALLEL.equals(qi.getExecutionType()) && executionType == null) {
						subscriber.onNext(qi);
						executionType = qi.getExecutionType();
					} else if (EXCLUSIVE.equals(qi.getExecutionType()) && executionType == null) {
						subscriber.onNext(qi);
						executionType = qi.getExecutionType();
					} else if (PARALLEL.equals(qi.getExecutionType()) && PARALLEL.equals(executionType)) {
						subscriber.onNext(qi);
						executionType = qi.getExecutionType();
					} else if (EXCLUSIVE.equals(qi.getExecutionType()) && PARALLEL.equals(executionType)) {
						break;
					} else if (PARALLEL.equals(qi.getExecutionType()) && EXCLUSIVE.equals(executionType)) {
						break;
					} else if (EXCLUSIVE.equals(qi.getExecutionType()) && EXCLUSIVE.equals(executionType)) {
						break;
					}
				}

				subscriber.onCompleted();

			}).doOnNext(qi -> qi.setStatus(QueueItemStatus.POPPED));
		}

		private List<SqlQueueItem> fetchNextPendingQueueItems(TransactionScope tx, int maxResults) {
			//noinspection unchecked
			return tx.session
				.createQuery("from SqlQueueItem qi where status=:status order by qi.ordinal")
				.setParameter("status", PENDING)
				.setMaxResults(maxResults)
				.list();
		}

		private ExecutionType fetchCurrentExecutionType(TransactionScope tx) {

			List currentExecutionStatus = tx.session
				.createQuery("select distinct qi.executionType from SqlQueueItem qi where status=:status")
				.setParameter("status", QueueItemStatus.POPPED)
				.list();

			if (currentExecutionStatus.isEmpty())
				return null;
			else if (currentExecutionStatus.size() == 1)
				return (ExecutionType) currentExecutionStatus.get(0);
			else
				throw new RuntimeException("Consistency error. There are popped items of different execution types.");
		}

		@Override
		public void close() {
			this.tx.close();
		}
	}
}

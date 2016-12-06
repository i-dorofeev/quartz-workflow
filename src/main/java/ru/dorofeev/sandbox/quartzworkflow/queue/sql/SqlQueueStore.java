package ru.dorofeev.sandbox.quartzworkflow.queue.sql;

import org.hibernate.SessionFactory;
import org.hibernate.StaleStateException;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.exception.ConstraintViolationException;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.NodeId;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueItem;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueItemStatus;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStoreException;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType;
import rx.Observable;

import javax.sql.DataSource;
import java.util.*;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.Collections.emptyList;
import static java.util.Optional.*;
import static ru.dorofeev.sandbox.quartzworkflow.NodeId.ANY_NODE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueItemStatus.PENDING;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.PARALLEL;
import static ru.dorofeev.sandbox.quartzworkflow.queue.sql.SqlQueueItem.fromNodeId;
import static ru.dorofeev.sandbox.quartzworkflow.utils.Contracts.*;
import static ru.dorofeev.sandbox.quartzworkflow.utils.SqlUtils.identifiersEqual;

@SuppressWarnings("JpaQlInspection")
public class SqlQueueStore implements QueueStore {

	private final SessionFactory sessionFactory;
	private final Queue<JobId> localQueue = new ConcurrentLinkedQueue<>();

	private final int fetchSize;

	public SqlQueueStore(DataSource dataSource, Class<? extends Dialect> dialect, String extraHibernateCfg, int fetchSize) {

		shouldNotBeNull(dataSource, "dataSource should be specified");
		shouldNotBeNull(dialect, "Hibernate dialect should be specified");
		mayBeNull(extraHibernateCfg);
		shouldBe(fetchSize > 0, "fetch size should be greater zero; current: %s", fetchSize);

		this.fetchSize = fetchSize;

		//noinspection deprecation
		this.sessionFactory = buildSessionFactory(dataSource, dialect, extraHibernateCfg);
	}

	private static SessionFactory buildSessionFactory(DataSource dataSource, Class<? extends Dialect> dialect, String extraHibernateCfg) {
		Configuration configuration = new Configuration()
			.addProperties(buildProperties(dataSource, dialect))
			.addAnnotatedClass(SqlQueueItem.class)
			.configure("/sqlQueueStore/sqlQueueStore.cfg.xml");

		if (extraHibernateCfg != null)
			configuration.configure(extraHibernateCfg);

		//noinspection deprecation
		return configuration.buildSessionFactory();
	}

	private static Properties buildProperties(DataSource ds, Class<? extends Dialect> dialect) {
		Properties properties = new Properties();
		properties.put("hibernate.connection.datasource", ds);
		properties.put("hibernate.dialect", dialect.getName());
		return properties;
	}

	private List<SqlQueueItem> popNext(String queueName, NodeId nodeId, int maxResults) {

		try (PopNextOperation op = newPopNextOperation()) {

			op.query(queueName, nodeId, maxResults);
			return op.getQueueItems();
		}
	}

	// "public" for testing purpose
	public PopNextOperation newPopNextOperation() {
		return new PopNextOperation();
	}

	@Override
	public QueueItem insertQueueItem(JobId jobId, String queueName, ExecutionType executionType, NodeId nodeId) throws QueueStoreException {

		shouldNotBeNull(jobId, "jobId should be supplied");
		shouldNotBeNull(queueName, "queueName should be supplied");
		shouldNotBeNull(executionType, "executionType should be supplied");
		shouldNotBeNull(nodeId, "nodeId should be specified. Use NodeId.ANY_NODE if it isn't required to run a job on a specific node.");

		try (TransactionScope tx = new TransactionScope(sessionFactory)) {

			SqlQueueItem queueItem = new SqlQueueItem(jobId.toString(), queueName, executionType, PENDING, fromNodeId(nodeId));
			tx.session.save(queueItem);

			tx.transaction.commit();

			return queueItem;
		} catch (ConstraintViolationException e) {
			if (identifiersEqual(e.getConstraintName(), SqlQueueItem.UK_QUEUE_JOBID_CONSTRAINT))
				throw new QueueStoreException(jobId + " is already enqueued", e);
			else
				throw new QueueStoreException("Couldn't insert queue item due to an external error.", e);
		}
	}

	@Override
	public Optional<JobId> popNextPendingQueueItem(String queueName, NodeId nodeId) {

		shouldNotBeNull(nodeId, "nodeId should be specified");
		shouldNotBe(ANY_NODE.equals(nodeId), "nodeId shouldn't be ANY_NODE");

		synchronized (localQueue) {
			JobId nextJobId = localQueue.poll();
			if (nextJobId != null)
				return of(nextJobId);

			popNext(queueName, nodeId, fetchSize).stream()
				.map(QueueItem::getJobId)
				.forEach(localQueue::add);

			return ofNullable(localQueue.poll());
		}
	}

	@Override
	public Optional<String> releaseQueueItem(JobId jobId) {

		shouldNotBeNull(jobId, "jobId should be specified");

		try (TransactionScope tx = new TransactionScope(sessionFactory)) {

			//noinspection unchecked
			List<SqlQueueItem> list = tx.session
				.createQuery("from SqlQueueItem where jobId=:jobId")
				.setParameter("jobId", jobId.toString())
				.list();

			if (list.isEmpty()) {
				tx.transaction.commit();
				return empty();
			} else if (list.size() == 1) {
				SqlQueueItem qi = list.get(0);
				tx.session.delete(qi);
				tx.transaction.commit();
				return ofNullable(qi.getQueueName());
			} else {
				throw new QueueStoreException("Unexpected count of queue items for the jobId " + jobId);
			}
		}
	}

	public void clear() {
		try (TransactionScope tx = new TransactionScope(sessionFactory)) {
			tx.session
				.createQuery("delete SqlQueueItem")
				.executeUpdate();

			tx.transaction.commit();
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

		public void query(String queueName, NodeId nodeId, int maxResults) {
			results = popNext(tx, queueName, nodeId, maxResults)
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

		private Observable<SqlQueueItem> popNext(TransactionScope tx, String queueName, NodeId nodeId, int maxResults) {

			HashMap<String, ExecutionType> executionTypesByQueue = fetchCurrentExecutionTypeByQueue(tx, queueName);

			return Observable.<SqlQueueItem>create(subscriber -> {
				List<SqlQueueItem> nextItems = queueName == null ?
					fetchNextPendingQueueItems(tx, nodeId, maxResults) :
					fetchNextPendingQueueItems(tx, queueName, nodeId, maxResults);

				for (SqlQueueItem qi : nextItems) {
					if (PARALLEL.equals(qi.getExecutionType()) && executionTypesByQueue.get(qi.getQueueName()) == null) {
						subscriber.onNext(qi);
						executionTypesByQueue.put(qi.getQueueName(), qi.getExecutionType());
					} else if (EXCLUSIVE.equals(qi.getExecutionType()) && executionTypesByQueue.get(qi.getQueueName()) == null) {
						subscriber.onNext(qi);
						executionTypesByQueue.put(qi.getQueueName(), qi.getExecutionType());
					} else if (PARALLEL.equals(qi.getExecutionType()) && PARALLEL.equals(executionTypesByQueue.get(qi.getQueueName()))) {
						subscriber.onNext(qi);
						executionTypesByQueue.put(qi.getQueueName(), qi.getExecutionType());
					} else if (EXCLUSIVE.equals(qi.getExecutionType()) && PARALLEL.equals(executionTypesByQueue.get(qi.getQueueName()))) {
						break;
					}
				}

				subscriber.onCompleted();

			}).doOnNext(qi -> qi.setStatus(QueueItemStatus.POPPED));
		}

		private List<SqlQueueItem> fetchNextPendingQueueItems(TransactionScope tx, NodeId nodeId, int maxResults) {
			//noinspection unchecked
			return tx.session
				.createQuery(
					"from SqlQueueItem qi " +
						"where status=:status " +
							"and (nodeId is null or nodeId=:nodeId)" +
						"order by qi.ordinal")
				.setParameter("status", PENDING)
				.setParameter("nodeId", fromNodeId(nodeId))
				.setMaxResults(maxResults)
				.list();
		}

		private List<SqlQueueItem> fetchNextPendingQueueItems(TransactionScope tx, String queueName, NodeId nodeId, int maxResults) {
			//noinspection unchecked
			return tx.session
				.createQuery(
					"from SqlQueueItem qi " +
						"where status=:status and ( (:queueName is not null and queueName=:queueName) or (:queueName is null) ) " +
							"and (nodeId is null or nodeId=:nodeId)" +
						"order by qi.ordinal")
				.setParameter("status", PENDING)
				.setParameter("queueName", queueName)
				.setParameter("nodeId", fromNodeId(nodeId))
				.setMaxResults(maxResults)
				.list();
		}

		private HashMap<String, ExecutionType> fetchCurrentExecutionTypeByQueue(TransactionScope tx, String queueName) {
			//noinspection unchecked
			List<Object[]> currentExecutionStatuses = tx.session
				.createQuery(
					"select distinct qi.queueName, qi.executionType " +
						"from SqlQueueItem qi " +
						"where status=:status and ( (:queueName is not null and queueName=:queueName) or (:queueName is null) )")
				.setParameter("status", QueueItemStatus.POPPED)
				.setParameter("queueName", queueName)
				.list();

			HashMap<String, ExecutionType> map = new HashMap<>();
			for (Object[] queueStatus: currentExecutionStatuses) {

				String queue = (String)queueStatus[0];
				ExecutionType executionType = (ExecutionType)queueStatus[1];

				ExecutionType oldValue = map.putIfAbsent(queue, executionType);
				if (oldValue != null)
					throw new RuntimeException("Consistency error. There are popped items of different execution types for queue [" + queue + "].");
			}
			return map;
		}

		@Override
		public void close() {
			this.tx.close();
		}
	}
}

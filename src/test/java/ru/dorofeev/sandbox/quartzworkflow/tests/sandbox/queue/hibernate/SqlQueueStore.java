package ru.dorofeev.sandbox.quartzworkflow.tests.sandbox.queue.hibernate;

import org.hibernate.SessionFactory;
import org.hibernate.StaleStateException;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType;
import rx.Observable;

import javax.sql.DataSource;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.emptyList;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.PARALLEL;
import static ru.dorofeev.sandbox.quartzworkflow.tests.sandbox.queue.hibernate.QueueItemStatus.PENDING;
import static ru.dorofeev.sandbox.quartzworkflow.tests.sandbox.queue.hibernate.QueueItemStatus.POPPED;

@SuppressWarnings("JpaQlInspection")
class SqlQueueStore {

	private final SessionFactory sessionFactory;

	SqlQueueStore(DataSource dataSource, Class<? extends Dialect> dialect, String extraHibernateCfg) {
		//noinspection deprecation
		sessionFactory = new Configuration()

			.addProperties(buildProperties(dataSource, dialect))

			.addAnnotatedClass(QueueItem.class)

			.configure("/sqlQueueStore.cfg.xml")
			.configure(extraHibernateCfg)

			.buildSessionFactory();
	}

	private Properties buildProperties(DataSource ds, Class<? extends Dialect> dialect) {
		Properties properties = new Properties();
		properties.put("hibernate.connection.datasource", ds);
		properties.put("hibernate.dialect", dialect.getName());
		return properties;
	}

	void enqueueItems(List<QueueItem> items) {
		try (TransactionScope tx = new TransactionScope(sessionFactory)) {

			for (QueueItem item: items)
				tx.session.save(item);

			tx.transaction.commit();
		}
	}

	List<QueueItem> popNext(int maxResults) {

		try (PopNextOperation op = newPopNextOperation()) {

			op.query(maxResults);
			return op.getQueueItems();
		}
	}

	PopNextOperation newPopNextOperation() {
		return new PopNextOperation();
	}

	void remove(int ordinal) {

		try (TransactionScope tx = new TransactionScope(sessionFactory)) {

			tx.session
				.createQuery("delete QueueItem where ordinal=:ordinal")
				.setParameter("ordinal", ordinal)
				.executeUpdate();

			tx.transaction.commit();
		}
	}

	public class PopNextOperation implements AutoCloseable {

		private final TransactionScope tx;

		private List<QueueItem> results;

		PopNextOperation() {
			this.tx = new TransactionScope(sessionFactory);
		}

		void query(int maxResults) {
			results = popNext(tx, maxResults)
				.toList().toBlocking().single();
		}

		List<QueueItem> getQueueItems() {
			if (results == null)
				throw new IllegalStateException("Invoke query() first.");

			try {
				this.tx.transaction.commit();
				return results;
			} catch (StaleStateException e) {
				// someone's already acquired queue items in a parallel transaction
				// (implemented using hibernate optimistic locking feature (QueueItem.version column))
				return emptyList();
			}
		}

		Observable<QueueItem> popNext(TransactionScope tx, int maxResults) {

			return Observable.<QueueItem>create(subscriber -> {
				List<QueueItem> nextItems = fetchNextPendingQueueItems(tx, maxResults);
				System.out.println(nextItems);

				ExecutionType executionType = fetchCurrentExecutionType(tx);
				System.out.println("Current execution type = " + executionType);

				for (QueueItem qi: nextItems) {
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

			}).doOnNext(qi -> qi.setStatus(POPPED));
		}

		private List<QueueItem> fetchNextPendingQueueItems(TransactionScope tx, int maxResults) {
			//noinspection unchecked
			return tx.session
				.createQuery("from QueueItem qi where status=:status order by qi.ordinal")
				.setParameter("status", PENDING)
				.setMaxResults(maxResults)
				.list();
		}

		private ExecutionType fetchCurrentExecutionType(TransactionScope tx) {

			List currentExecutionStatus = tx.session
				.createQuery("select distinct qi.executionType from QueueItem qi where status=:status")
				.setParameter("status", POPPED)
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

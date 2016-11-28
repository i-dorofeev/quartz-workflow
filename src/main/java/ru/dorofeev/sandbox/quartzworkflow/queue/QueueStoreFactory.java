package ru.dorofeev.sandbox.quartzworkflow.queue;

import org.hibernate.dialect.Dialect;
import ru.dorofeev.sandbox.quartzworkflow.queue.sql.SqlQueueStore;
import ru.dorofeev.sandbox.quartzworkflow.utils.entrypoint.API;

import javax.sql.DataSource;

@API
public class QueueStoreFactory {

	@API
	public static QueueStore inMemoryQueueStore() {
		return new InMemoryQueueStore();
	}

	@API
	public static QueueStore sqlQueueStore(DataSource dataSource, Class<? extends Dialect> hibernateDialect, String extraHibernateCfg) {
		return new SqlQueueStore(dataSource, hibernateDialect, extraHibernateCfg, 5);
	}
}

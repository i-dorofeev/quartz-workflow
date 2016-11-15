package ru.dorofeev.sandbox.quartzworkflow.queue;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import ru.dorofeev.sandbox.quartzworkflow.engine.EngineException;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

class QueueSqlStore implements QueueStore {

	QueueSqlStore(String dataSourceUrl) {
		prepareDatabase(dataSourceUrl);
	}

	private void prepareDatabase(String dataSourceUrl) {
		try {
			JdbcConnection h2Connection = new JdbcConnection(DriverManager.getConnection(dataSourceUrl));
			Database db = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(h2Connection);
			Liquibase liquibase = new Liquibase("engine.db.changelog.xml", new CustomClassLoaderResourceAccessor(QueueSqlStore.class.getClassLoader()), db);
			liquibase.update(new Contexts());
		} catch (SQLException | LiquibaseException e) {
			throw new EngineException(e);
		}
	}

	@Override
	public void insertQueueItem(TaskId taskId, String queueName, QueueingOption.ExecutionType executionType) throws QueueStoreException {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Optional<TaskId> getNextPendingQueueItem(String queueName) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Optional<String> removeQueueItem(TaskId taskId) {
		throw new UnsupportedOperationException("Not implemented yet");
	}
}

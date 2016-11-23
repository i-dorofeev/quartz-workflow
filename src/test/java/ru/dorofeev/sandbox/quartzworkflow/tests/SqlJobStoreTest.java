package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStore;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;

import static org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType.HSQL;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.JobStoreFactory.sqlJobStore;
import static ru.dorofeev.sandbox.quartzworkflow.serialization.SerializationFactory.jsonSerialization;

public class SqlJobStoreTest extends AbstractJobStoreTest {

	private static SerializedObjectFactory serialization;
	private static EmbeddedDatabase db;
	private static JobStore store;

	@BeforeClass
	public static void beforeClass() {

		serialization = jsonSerialization();

		db = new EmbeddedDatabaseBuilder()
			.generateUniqueName(true)
			.setType(HSQL)
			.build();

		store = sqlJobStore(db).call(serialization);
	}

	@AfterClass
	public static void afterClass() {
		db.shutdown();
	}

	@Override
	protected JobStore getStore() {
		return store;
	}

	@Override
	protected SerializedObjectFactory getSerializedObjectFactory() {
		return serialization;
	}
}

package ru.dorofeev.sandbox.quartzworkflow.tests.jobstore;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStore;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.TestHSqlJobStore;

import static ru.dorofeev.sandbox.quartzworkflow.jobs.JobStoreFactory.sqlJobStore;
import static ru.dorofeev.sandbox.quartzworkflow.serialization.SerializationFactory.jsonSerialization;

@SuppressWarnings("unused")
public class SqlJobStoreTest extends AbstractJobStoreTest {

	private static SerializedObjectFactory serialization;
	private static JobStore jobStore;
	private static TestHSqlJobStore testHSqlJobStore;

	@BeforeClass
	public static void beforeClass() {

		serialization = jsonSerialization();
		testHSqlJobStore = new TestHSqlJobStore();
		jobStore = sqlJobStore(testHSqlJobStore.getDataSource()).call(serialization);
	}

	@AfterClass
	public static void afterClass() {
		testHSqlJobStore.shutdown();
	}

	@Override
	protected JobStore getStore() {
		return jobStore;
	}

	@Override
	protected SerializedObjectFactory getSerializedObjectFactory() {
		return serialization;
	}
}

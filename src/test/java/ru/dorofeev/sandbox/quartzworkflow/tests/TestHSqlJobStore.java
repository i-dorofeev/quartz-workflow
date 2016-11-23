package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;

import javax.sql.DataSource;

import static org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType.HSQL;

public class TestHSqlJobStore {

	private final EmbeddedDatabase db;

	public TestHSqlJobStore() {

		db = new EmbeddedDatabaseBuilder()
			.generateUniqueName(true)
			.setType(HSQL)
			.build();
	}

	public void shutdown() {
		db.shutdown();
	}

	public DataSource getDataSource() {
		return db;
	}
}

package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;

import javax.sql.DataSource;

import static org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType.HSQL;

class TestHSqlJobStore {

	private final EmbeddedDatabase db;

	TestHSqlJobStore() {

		db = new EmbeddedDatabaseBuilder()
			.generateUniqueName(true)
			.setType(HSQL)
			.build();
	}

	void shutdown() {
		db.shutdown();
	}

	DataSource getDataSource() {
		return db;
	}
}

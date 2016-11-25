package ru.dorofeev.sandbox.quartzworkflow.tests.utils;

import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;

import javax.sql.DataSource;

import static org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType.HSQL;

public class HSqlDb {

	private final EmbeddedDatabase db;

	public HSqlDb() {

		db = new EmbeddedDatabaseBuilder()
			.generateUniqueName(true)
			.setType(HSQL)
			.build();
	}

	public HSqlDb(String name) {

		db = new EmbeddedDatabaseBuilder()
			.setName(name)
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

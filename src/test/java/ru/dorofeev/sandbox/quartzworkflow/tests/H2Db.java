package ru.dorofeev.sandbox.quartzworkflow.tests;

import java.io.File;

class H2Db {

	private final String path;

	H2Db(String path) {
		this.path = path;
	}

	String jdbcUrl() {
		return "jdbc:h2:" + path;
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	void deleteDb() {
		new File(path + ".mv.db").delete();
		new File(path + ".trace.db").delete();
	}
}

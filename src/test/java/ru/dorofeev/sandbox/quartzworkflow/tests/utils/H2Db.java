package ru.dorofeev.sandbox.quartzworkflow.tests.utils;

import java.io.File;

public class H2Db {

	private final String path;

	public H2Db(String path) {
		this.path = path;
	}

	public String jdbcUrl() {
		return "jdbc:h2:" + path;
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	public void deleteDb() {
		new File(path + ".mv.db").delete();
		new File(path + ".trace.db").delete();
	}
}

package ru.dorofeev.sandbox.quartzworkflow.utils;

import rx.functions.Func0;

public class SqlBuilder {

	private final StringBuilder sb = new StringBuilder();

	public SqlBuilder insertInto(String tableName) {
		sb.append(" insert into ").append(tableName);
		return this;
	}

	public SqlBuilder values(String... values) {
		sb.append(" values (").append(commaDelimited(values)).append(")");
		return this;
	}

	public SqlBuilder select(String... columnNames) {
		sb.append(" select ").append(commaDelimited(columnNames));
		return this;
	}

	public SqlBuilder from(String tableName) {
		sb.append(" from ").append(tableName);
		return this;
	}

	public SqlBuilder where(String condition) {
		return where(() -> true, condition);
	}

	public SqlBuilder where(Func0<Boolean> predicate, String condition) {
		if (predicate.call())
			sb.append(" where (").append(condition).append(")");
		return this;
	}

	public String sql() {
		return sb.toString();
	}

	public static String sqlEquals(String left, String right) {
		return left + " = " + right;
	}

	private static String commaDelimited(String... items) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < items.length; i++) {
			if (i != 0) sb.append(',');
			sb.append(items[i]);
		}

		return sb.toString();
	}
}

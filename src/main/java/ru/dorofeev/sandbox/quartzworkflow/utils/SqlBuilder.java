package ru.dorofeev.sandbox.quartzworkflow.utils;

import rx.functions.Func0;

import static java.util.Arrays.stream;

public class SqlBuilder {

	private final StringBuilder sb;

	private SqlBuilder(String initialString) {
		this.sb = new StringBuilder(initialString);
	}

	private SqlBuilder() {
		this("");
	}

	public static SqlBuilder insertInto(String tableName) {
		SqlBuilder sqlBuilder = new SqlBuilder();
		sqlBuilder.sb.append(" insert into ").append(tableName);
		return sqlBuilder;
	}

	public static SqlBuilder insertInto(String tableName, SqlBuilder values) {
		SqlBuilder sqlBuilder = new SqlBuilder();
		sqlBuilder.sb.append(" insert into ").append(tableName).append(values.sql());
		return sqlBuilder;
	}

	public SqlBuilder values(String... values) {
		sb.append(" values (").append(commaDelimited(values)).append(")");
		return this;
	}

	public static SqlBuilder select(String... columnNames) {
		SqlBuilder sqlBuilder = new SqlBuilder();
		sqlBuilder.sb.append(" select ").append(commaDelimited(columnNames));
		return sqlBuilder;
	}

	public SqlBuilder from(String tableName) {
		sb.append(" from ").append(tableName);
		return this;
	}

	public SqlBuilder from(String tableName, String alias) {
		sb.append(" from ").append(tableName).append(" ").append(alias);
		return this;
	}

	public SqlBuilder join(String tableName, String alias) {
		sb.append(" join ").append(tableName).append(" ").append(alias);
		return this;
	}

	public SqlBuilder on(String condition) {
		sb.append(" on ").append(condition);
		return this;
	}

	public SqlBuilder on(SqlBuilder builder) {
		return on(builder.sql());
	}

	public SqlBuilder where(String condition) {
		return where(() -> true, condition);
	}

	public SqlBuilder where(SqlBuilder builder) {
		return where(builder.sql());
	}

	public SqlBuilder where(Func0<Boolean> predicate, String condition) {
		if (predicate.call())
			sb.append(" where (").append(condition).append(")");
		return this;
	}

	public SqlBuilder or(String expr) {
		sb.append(" or ").append(expr);
		return this;
	}

	public SqlBuilder or(SqlBuilder builder) {
		return or(builder.sql());
	}

	public SqlBuilder and(String expr) {
		sb.append(" and ").append(expr);
		return this;
	}

	public SqlBuilder and(SqlBuilder builder) {
		return and(builder.sql());
	}

	public static SqlBuilder update(String tableName) {
		SqlBuilder sqlBuilder = new SqlBuilder();
		sqlBuilder.sb.append(" update ").append(tableName);
		return sqlBuilder;
	}

	public SqlBuilder set(String... assignments) {
		sb.append(" set ").append(commaDelimited(assignments));
		return this;
	}

	public SqlBuilder set(SqlBuilder... assignments) {
		return set(stream(assignments)
			.map(SqlBuilder::sql)
			.toArray(String[]::new));
	}

	public String sql() {
		return sb.toString();
	}

	public static SqlBuilder sqlEquals(String left, String right) {
		SqlBuilder sqlBuilder = new SqlBuilder();
		sqlBuilder.sb.append(left + " = " + right);
		return sqlBuilder;
	}

	public static SqlBuilder isNull(String operand) {
		SqlBuilder sqlBuilder = new SqlBuilder();
		sqlBuilder.sb.append(operand).append(" is null");
		return sqlBuilder;
	}

	public static SqlBuilder expr(SqlBuilder builder) {
		SqlBuilder sqlBuilder = new SqlBuilder();
		sqlBuilder.sb.append("(").append(builder.sql()).append(")");
		return sqlBuilder;
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

package ru.dorofeev.sandbox.quartzworkflow.utils;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;

import javax.sql.DataSource;
import java.sql.SQLException;

public class SqlUtils {

	public static void liquibaseUpdate(DataSource dataSource, String changeLogFile) throws LiquibaseException, SQLException {
		Database db = null;

		try {
			JdbcConnection jdbcConnection = new JdbcConnection(dataSource.getConnection());		// will be closed by db.close() in the finally block
			db = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(jdbcConnection);

			Liquibase liquibase = new Liquibase(changeLogFile, new CustomClassLoaderResourceAccessor(SqlUtils.class.getClassLoader()), db);
			liquibase.update(new Contexts());

		} finally {
			if (db != null)
				db.close();
		}
	}
}

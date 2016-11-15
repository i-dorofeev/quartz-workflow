package ru.dorofeev.sandbox.quartzworkflow;

import java.sql.Driver;

public class EngineFactory {

	public static Engine create(Class<? extends Driver> sqlDriver, String dataSourceUrl) {
		return new EngineImpl(sqlDriver, dataSourceUrl);
	}
}

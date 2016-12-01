package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.utils.Clock;

public class JobRepositoryFactory {

	public static JobRepository create(JobStore store, Clock clock) {
		return new JobRepositoryImpl(store, clock);
	}
}

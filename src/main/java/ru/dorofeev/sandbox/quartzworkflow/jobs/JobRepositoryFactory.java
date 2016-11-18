package ru.dorofeev.sandbox.quartzworkflow.jobs;

public class JobRepositoryFactory {

	public static JobRepository create(JobStore store) {
		return new JobRepositoryImpl(store);
	}
}

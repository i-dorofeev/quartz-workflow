package ru.dorofeev.sandbox.quartzworkflow.jobs;

public class JobRepositoryFactory {

	public static JobRepository create() {
		return new JobRepositoryImpl();
	}
}

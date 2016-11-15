package ru.dorofeev.sandbox.quartzworkflow;

public interface Executable {

	void execute(JobDataMap args) throws Throwable;
}

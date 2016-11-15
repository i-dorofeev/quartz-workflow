package ru.dorofeev.sandbox.quartzworkflow.execution;

import ru.dorofeev.sandbox.quartzworkflow.JobDataMap;

public interface Executable {

	void execute(JobDataMap args) throws Throwable;
}

package ru.dorofeev.sandbox.quartzworkflow.execution;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;

public interface Executable {

	void execute(JobId jobId, SerializedObject serializedArgs) throws Throwable;
}

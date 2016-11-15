package ru.dorofeev.sandbox.quartzworkflow.execution;

import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;

public interface Executable {

	void execute(TaskId taskId, SerializedObject serializedArgs) throws Throwable;
}

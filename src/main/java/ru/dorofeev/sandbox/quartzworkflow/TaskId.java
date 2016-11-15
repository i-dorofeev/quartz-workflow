package ru.dorofeev.sandbox.quartzworkflow;

import java.util.UUID;

public class TaskId {

	public static TaskId taskId(String id) {
		return new TaskId(id);
	}

	public static TaskId createUniqueTaskId() {
		return new TaskId(UUID.randomUUID().toString());
	}

	private final String value;

	public TaskId(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		TaskId taskId = (TaskId) o;

		return value != null ? value.equals(taskId.value) : taskId.value == null;

	}

	@Override
	public int hashCode() {
		return value != null ? value.hashCode() : 0;
	}
}

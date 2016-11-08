package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.utils.Key;

class TaskId {

	static TaskId createUniqueTaskId() {
		return new TaskId(Key.createUniqueName(TaskId.class.getName()));
	}

	private final String value;

	TaskId(String value) {
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
package ru.dorofeev.sandbox.quartzworkflow;

import java.util.UUID;

public class JobId {

	public static JobId taskId(String id) {
		return new JobId(id);
	}

	public static JobId createUniqueTaskId() {
		return new JobId(UUID.randomUUID().toString());
	}

	private final String value;

	public JobId(String value) {
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

		JobId jobId = (JobId) o;

		return value != null ? value.equals(jobId.value) : jobId.value == null;

	}

	@Override
	public int hashCode() {
		return value != null ? value.hashCode() : 0;
	}
}

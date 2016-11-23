package ru.dorofeev.sandbox.quartzworkflow;

public class JobId {

	public static JobId jobId(String id) {
		return new JobId(id);
	}

	private final String value;

	public JobId(String value) {
		if (value == null)
			throw new IllegalArgumentException("value must be non null");

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

		return value.equals(jobId.value);

	}

	@Override
	public int hashCode() {
		return value.hashCode();
	}
}

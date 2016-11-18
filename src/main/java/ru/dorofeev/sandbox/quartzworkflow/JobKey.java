package ru.dorofeev.sandbox.quartzworkflow;

public class JobKey {

	private final String value;

	public JobKey(String value) {
		this.value = value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		JobKey jobKey = (JobKey) o;

		return value.equals(jobKey.value);

	}

	@Override
	public int hashCode() {
		return value.hashCode();
	}

	@Override
	public String toString() {
		return value;
	}
}

package ru.dorofeev.sandbox.quartzworkflow.tests.jobstore;

import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;

class StubArgs implements Serializable {

	private final String value;

	StubArgs(String value) {
		this.value = value;
	}

	@Override
	public void serializeTo(SerializedObject serializedObject) {
		serializedObject.addString("value", value);
	}

	@Override
	public String toString() {
		return "StubArgs{" +
			"value='" + value + '\'' +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		StubArgs stubArgs = (StubArgs) o;

		return value != null ? value.equals(stubArgs.value) : stubArgs.value == null;

	}

	@Override
	public int hashCode() {
		return value != null ? value.hashCode() : 0;
	}
}

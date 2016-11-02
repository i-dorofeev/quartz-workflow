package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.utils.Key;

public class LocalId {

	public static LocalId createUniqueLocalId() {
		return new LocalId(Key.createUniqueName(LocalId.class.getName()));
	}

	private final String value;

	public LocalId(String value) {
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

		LocalId localId = (LocalId) o;

		return value != null ? value.equals(localId.value) : localId.value == null;

	}

	@Override
	public int hashCode() {
		return value != null ? value.hashCode() : 0;
	}
}

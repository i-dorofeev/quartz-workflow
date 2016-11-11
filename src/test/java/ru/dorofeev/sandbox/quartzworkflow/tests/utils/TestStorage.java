package ru.dorofeev.sandbox.quartzworkflow.tests.utils;

import java.util.ArrayList;
import java.util.List;

public class TestStorage<T> {

	private final List<T> storage = new ArrayList<>();
	private int assertionIdx = 0;

	public void add(T item) {
		synchronized (storage) {
			storage.add(item);
		}
	}

	public <O extends T> void assertNextItemsOfTypeMin(Class<O> klass, int min) {
		int idx = 0;
		while (true) {
			if (storage.size() <= assertionIdx && idx < min)
				throw new AssertionError("Reached the end of the storage but got only " + idx + " items of type " + klass);

			if (storage.size() <= assertionIdx && idx >= min)
				return;

			T next = storage.get(assertionIdx);
			if (idx < min && !next.getClass().equals(klass))
				throw new AssertionError("Unexpected item[" + assertionIdx + "]=" + next + " when expected item of " + klass);

			if (idx >= min && !next.getClass().equals(klass))
				return;

			idx++;
			assertionIdx++;
		}
	}

	public <O extends T> void assertNextItemsOfTypeExact(Class<O> klass, int expectedCount) {

		int count = 0;

		while (count != expectedCount) {
			if (storage.size() <= assertionIdx)
				throw new AssertionError("Reached the end of the storage but got only " + count + " items of type " + klass);

			T next = storage.get(assertionIdx);
			if (next.getClass().equals(klass))
				count++;

			assertionIdx++;
		}
	}

	@Override
	public String toString() {
		return "TestStorage{" +
			"storage=" + storage +
			'}';
	}
}

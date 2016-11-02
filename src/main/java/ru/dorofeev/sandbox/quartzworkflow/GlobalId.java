package ru.dorofeev.sandbox.quartzworkflow;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public class GlobalId {

	private static final String PATH_DELIMITER = "/";

	public static GlobalId fromString(String value) {
		return new GlobalId(splitId(value));
	}

	private static List<LocalId> splitId(String id) {
		return stream(id.split(PATH_DELIMITER))
			.map(LocalId::new)
			.collect(toList());
	}

	private static String concat(List<LocalId> ids) {
		return String.join(
			PATH_DELIMITER,
			ids.stream()
				.map((localId) -> localId.toString())
				.collect(toList())
		);
	}

	private final List<LocalId> ids;

	public GlobalId(List<LocalId> localIds) {
		this.ids = new ArrayList<>(localIds);
	}

	<T> T traverse(T initialValue, BiFunction<T, ? super LocalId, T> accumulator) {

		T value = initialValue;
		for (LocalId localId: ids) {
			value = accumulator.apply(value, localId);
		}

		return value;
	}

	@Override
	public String toString() {
		return concat(ids);
	}
}

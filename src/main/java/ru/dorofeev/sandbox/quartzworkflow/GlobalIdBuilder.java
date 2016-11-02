package ru.dorofeev.sandbox.quartzworkflow;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.reverse;

public class GlobalIdBuilder {

	private final List<LocalId> ids = new ArrayList<>(1);

	public GlobalIdBuilder addParent(LocalId localId) {
		ids.add(localId);
		return this;
	}

	GlobalId build() {
		reverse(ids);
		return new GlobalId(new ArrayList<>(ids));
	}
}

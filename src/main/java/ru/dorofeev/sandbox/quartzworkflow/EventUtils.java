package ru.dorofeev.sandbox.quartzworkflow;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class EventUtils {

	public static List<Event> noEvents() {
		return Collections.emptyList();
	}

	public static List<Event> events(Event... events) {
		return Arrays.asList(events);
	}
}

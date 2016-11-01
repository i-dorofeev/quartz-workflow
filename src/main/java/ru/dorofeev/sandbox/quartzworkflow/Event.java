package ru.dorofeev.sandbox.quartzworkflow;

import com.google.gson.Gson;

public class Event {

	private static final Gson gson = new Gson();

	static String toJson(Event event) {
		return gson.toJson(event);
	}

	static Event toEvent(String eventClassName, String json) throws ClassNotFoundException {
		//noinspection unchecked
		Class<? extends Event> eventClass = (Class<? extends Event>) Class.forName(eventClassName);
		return gson.fromJson(json, eventClass);
	}

}

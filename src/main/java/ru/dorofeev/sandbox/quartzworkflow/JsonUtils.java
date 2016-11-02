package ru.dorofeev.sandbox.quartzworkflow;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

class JsonUtils {

	private static final Gson gson = new Gson();
	private static final Gson prettyGson = new GsonBuilder().setPrettyPrinting().create();

	static String toJson(Object object) {
		return gson.toJson(object);
	}

	static <T> T toObject(String className, String json) throws ClassNotFoundException {
		//noinspection unchecked
		Class<T> eventClass = (Class<T>) Class.forName(className);
		return gson.fromJson(json, eventClass);
	}

	static String toPrettyJson(Object object) {
		return prettyGson.toJson(object);
	}
}

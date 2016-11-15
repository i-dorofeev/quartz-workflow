package ru.dorofeev.sandbox.quartzworkflow.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonUtils {

	private static final Gson prettyGson = new GsonBuilder().setPrettyPrinting().create();

	public static String toPrettyJson(Object object) {
		return prettyGson.toJson(object);
	}
}

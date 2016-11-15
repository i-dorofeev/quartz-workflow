package ru.dorofeev.sandbox.quartzworkflow.serialization;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

class JsonSerializedObject implements SerializedObject {

	private static Gson gson = new Gson();

	private JsonObject jsonObject = new JsonObject();

	@Override
	public void addString(String name, String value) {
		jsonObject.add(name, new JsonPrimitive(value));
	}

	@Override
	public void addUntypedObject(String name, Object obj) {
		JsonObject jo = new JsonObject();
		jo.add("type", new JsonPrimitive(obj.getClass().getName()));
		jo.add("value", gson.toJsonTree(obj));

		this.jsonObject.add(name, jo);
	}

	@Override
	public void addTypedObject(String name, Object obj) {
		jsonObject.add(name, gson.toJsonTree(obj));
	}

	@Override
	public String build() {
		return jsonObject.toString();
	}

	@Override
	public String getString(String name) {
		return jsonObject.getAsJsonPrimitive(name).getAsString();
	}

	@Override
	public <T> T getUntypedObject(String name, Class<T> type) {
		try {
			JsonObject jo = jsonObject.getAsJsonObject(name);

			String typeStr = jo.getAsJsonPrimitive("type").getAsString();
			JsonObject valueJo = jo.getAsJsonObject("value");

			Class<?> klass = Class.forName(typeStr);
			return (T)gson.fromJson(valueJo, klass);
		} catch (ClassNotFoundException e) {
			throw new SerializerException(e);
		}
	}

	@Override
	public <T> T getTypedObject(String name, Class<T> type) {
		JsonObject jo = jsonObject.getAsJsonObject(name);
		return gson.fromJson(jo, type);
	}
}

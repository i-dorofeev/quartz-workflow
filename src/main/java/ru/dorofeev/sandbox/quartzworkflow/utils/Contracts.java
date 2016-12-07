package ru.dorofeev.sandbox.quartzworkflow.utils;

public class Contracts {

	public static void shouldNotBeNull(Object arg, String message, Object... args) {
		if (arg == null)
			throw new IllegalArgumentException(format(message, args));
	}

	public static void shouldNotBeEmpty(String str, String message, Object... args) {
		if (str == null)
			throw new IllegalArgumentException(format(message, args, "str == null"));

		if (str.trim().isEmpty())
			throw new IllegalArgumentException(format(message, args, "str == [" + str + "]"));
	}

	public static void shouldBe(boolean condition, String message, Object... args) {
		if (!condition)
			throw new IllegalArgumentException(format(message, args));
	}

	public static void shouldNotBe(boolean condition, String message, Object... args) {
		if (condition)
			throw new IllegalArgumentException(format(message, args));
	}

	@SuppressWarnings("unused")
	public static void mayBeNull(Object arg) {
		// just a specification
	}

	private static String format(String message, Object[] args, String extra) {
		return String.format("Contract violation. " + message + " / " + extra, args);
	}

	private static String format(String message, Object[] args) {
		return String.format("Contract violation. " + message, args);
	}
}

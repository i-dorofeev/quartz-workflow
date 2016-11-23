package ru.dorofeev.sandbox.quartzworkflow.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtils {

	public static String toString(Throwable ex) {
		if (ex == null)
			return null;

		StringWriter stringWriter = new StringWriter();
		ex.printStackTrace(new PrintWriter(stringWriter));
		return stringWriter.toString();
	}
}

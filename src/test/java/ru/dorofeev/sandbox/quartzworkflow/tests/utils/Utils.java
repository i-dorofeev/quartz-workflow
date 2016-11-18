package ru.dorofeev.sandbox.quartzworkflow.tests.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

public class Utils {

	public static String exceptionToString(Throwable e) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter);
		e.printStackTrace(printWriter);
		return stringWriter.toString();
	}
}

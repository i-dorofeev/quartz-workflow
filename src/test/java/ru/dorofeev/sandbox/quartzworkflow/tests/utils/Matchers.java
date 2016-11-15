package ru.dorofeev.sandbox.quartzworkflow.tests.utils;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;

public class Matchers {

	public static <T> org.hamcrest.Matcher<Observable<T>> hasOnlyOneItem() {
		return hasOnlyOneItem(null, item -> true);
	}

	public static <T> org.hamcrest.Matcher<Observable<T>> hasOnlyOneItem(String descr, Func1<? super T, Boolean> predicate) {
		return new BaseMatcher<Observable<T>>() {
			@Override
			public boolean matches(Object item) {
				List<T> list = getList(item);
				System.out.println(list);
				return list.size() == 1;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("only one item");
				if (descr != null)
					description.appendText(" ").appendText(descr);
			}

			@Override
			public void describeMismatch(Object item, Description description) {
				List<T> list = getList(item);
				description.appendText(list.toString());
			}

			private List<T> getList(Object item) {
				//noinspection unchecked
				Observable<T> observable = (Observable<T>) item;
				return observable.filter(predicate).toList().toBlocking().single();
			}
		};
	}
}

package ru.dorofeev.sandbox.quartzworkflow;

import rx.Observable;
import rx.Observer;
import rx.Subscriber;

import java.util.ArrayList;
import java.util.List;

public class ObservableHolder<T> implements Observer<T> {

	private final Observable<T> observable;
	private final List<Subscriber<? super T>> subscriberList;

	public ObservableHolder() {
		this.subscriberList = new ArrayList<>();
		this.observable = Observable.create(subscriberList::add);
	}

	public Observable<T> getObservable() {
		return observable;
	}

	@Override
	public void onNext(T item) {
		subscriberList.forEach(s -> s.onNext(item));
	}

	@Override
	public void onCompleted() {
		subscriberList.forEach(Observer::onCompleted);
	}

	@Override
	public void onError(Throwable e) {
		subscriberList.forEach(s -> s.onError(e));
	}
}

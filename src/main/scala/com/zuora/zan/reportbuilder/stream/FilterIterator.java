package com.zuora.zan.reportbuilder.stream;

import java.util.Iterator;

public class FilterIterator<T> implements Iterator<T> {
	private Iterator<T> iterator;
	private Predicate<T> predicate;
	private T current;

	public FilterIterator(Iterator<T> iterator, Predicate<T> predicate) {
		this.iterator = iterator;
		this.predicate = predicate;
	}

	private T findNext() {
		while (iterator.hasNext()) {
			T o = iterator.next();
			if (predicate.apply(o))
				return o;
		}
		return null;
	}

	@Override
	public boolean hasNext() {
		current = findNext();
		return current != null;
	}

	@Override
	public T next() {
		return current;
	}

	@Override
	public void remove() {
		iterator.remove();
	}
}
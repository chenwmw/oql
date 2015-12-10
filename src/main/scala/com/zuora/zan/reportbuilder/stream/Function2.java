package com.zuora.zan.reportbuilder.stream;

public interface Function2<R, T1, T2> {
	R apply(T1 o1, T2 o2);
}

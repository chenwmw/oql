package com.zuora.zan.reportbuilder.stream;

public interface Function3<R, T1, T2, T3> {
	R apply(T1 o1, T2 o2, T3 o);
}

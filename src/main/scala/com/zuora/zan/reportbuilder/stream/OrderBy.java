package com.zuora.zan.reportbuilder.stream;

public class OrderBy {
	private String field;
	private boolean ascend;

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public boolean isAscend() {
		return ascend;
	}

	public void setAscend(boolean ascend) {
		this.ascend = ascend;
	}
}

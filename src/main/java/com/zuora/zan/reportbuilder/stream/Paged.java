package com.zuora.zan.reportbuilder.stream;

import java.util.List;

public class Paged<T> {
	private int totalCount;
	private int totalPage;
	private int pageNo;
	private int pageSize;
	private List<T> elements;

	public Paged() {
	}

	public Paged(int totalCount, int totalPage, int pageNo, int pageSize, List<T> elements) {
		this.totalCount = totalCount;
		this.totalPage = totalPage;
		this.pageNo = pageNo;
		this.pageSize = pageSize;
		this.elements = elements;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}

	public int getTotalPage() {
		return totalPage;
	}

	public void setTotalPage(int totalPage) {
		this.totalPage = totalPage;
	}

	public int getPageNo() {
		return pageNo;
	}

	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public List<T> getElements() {
		return elements;
	}

	public void setElements(List<T> elements) {
		this.elements = elements;
	}
}

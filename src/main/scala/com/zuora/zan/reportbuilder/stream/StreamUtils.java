package com.zuora.zan.reportbuilder.stream;

import java.util.Collections;
import java.util.List;

public class StreamUtils {

	public static <T> List<T> sort(List<T> list, Class<T> beanClass, OrderBy[] orderBy) {
		Collections.sort(list, new FieldComparator<T>(new BeanSortAdapter<T>(beanClass), orderBy));
		return list;
	}

	public static <T> List<T> sort(List<T> list, SortAdapter<T> adapter, OrderBy[] orderBy) {
		Collections.sort(list, new FieldComparator<T>(adapter, orderBy));
		return list;
	}

	@SuppressWarnings("unchecked")
	public static <T> Paged<T> pageList(Paging paging, List<T> list) {
		int totalCount = list.size();
		int pageNo = paging.getPageNo();
		int pageSize = paging.getPageSize();
		int rest = totalCount % pageSize;
		int totalPage = totalCount / pageSize + (rest == 0 ? 0 : 1);
		int startIndex = pageNo * pageSize;
		int endIndex = startIndex + pageSize;
		if (startIndex >= list.size()) {
			return new Paged<T>(totalCount, totalPage, pageNo, 0, Collections.EMPTY_LIST);
		}
		if (endIndex > list.size()) {
			endIndex = list.size();
			pageSize = endIndex - startIndex;
		}
		return new Paged<T>(totalCount, totalPage, pageNo, pageSize, list.subList(startIndex, endIndex));
	}
}

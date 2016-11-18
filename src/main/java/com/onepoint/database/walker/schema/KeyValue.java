package com.onepoint.database.walker.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KeyValue {
	
	private final List<Object> values;
	
	public KeyValue() {
		values = new ArrayList<>();
	}
	
	public KeyValue(Object... values) {
		this.values = Arrays.asList(values);
	}

	public List<Object> list() {
		return values;
	}

	public void add(Object object) {
		values.add(object);
	}

	@Override
	public String toString() {
		return "KeyValue [" + values + "]";
	}

}

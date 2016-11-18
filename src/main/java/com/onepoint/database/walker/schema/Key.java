package com.onepoint.database.walker.schema;

import java.util.ArrayList;
import java.util.List;

public class Key {
	
	private final String name;
	private final Table table;
	private final List<Integer> columnIdxs;
	
	public Key(String name, Table table) {
		this.name = name;
		this.table = table;
		this.columnIdxs = new ArrayList<>();
	}
	
	public void addColumn(String columnName) {
		Integer columnIdx = table.getColumnIdx(columnName);
		columnIdxs.add(columnIdx);
	}

	public Table getTable() {
		return table;
	}
	
	public Schema getSchema() {
		return table.getSchema();
	}
	
	public boolean isName(String keyName) {
		return this.name.equals(keyName);
	}
	
	public String getSqlSelect() {
		return table.getSqlSelect() + getSqlWhere();
	}
	
	public String getSqlWhere() {
		StringBuilder sb = new StringBuilder(" WHERE 1=1 ");
		for (int columnIdx : columnIdxs) {
			sb.append(" AND ").append(table.getColumnName(columnIdx)).append(" = ?");
		}
		return sb.toString();
	}
	
	public List<Integer> getColumnIdxs() {
		return columnIdxs;
	}
	
	public boolean contains(int i) {
		return columnIdxs.contains(i);
	}

	@Override
	public String toString() {
		return "Key [name=" + name + ", table=" + table.getName() + ", columnIdxs=" + columnIdxs + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Key other = (Key) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	
	

}

package org.peekmoon.database.walker.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Table {
	
	private final Schema schema;
	private final String name;
	private final List<String> columnNames;
	private PrimaryKey key;

	public Table(Schema schema, String tableName) {
		this.schema = schema;
		this.name = tableName;
		this.columnNames = new ArrayList<>();
	}
	
	public void addColumn(String columnName) {
		columnNames.add(columnName);
	}
	
	public void addPrimaryKey(String primaryKeyName) {
		this.key = new PrimaryKey(primaryKeyName, this);
	}

	public void addPkColumn(String columnName) {
		key.addColumn(columnName);
	}

	public boolean havePrimaryKey(String pkName) {
		return (key!=null && key.isName(pkName));
	}

	public PrimaryKey getPrimaryKey() {
		return key;
	}

	public boolean is(String tableName) {
		return name.equals(tableName);
	}

	public String getName() {
		return name;
	}
	
	public KeyValue getKeyValue(Table affAffaire) {
		return new KeyValue();
	}
	
	public List<String> getColumnNames() {
		return Collections.unmodifiableList(columnNames);
	}

	public Integer getColumnIdx(String columnName) {
		int idx = columnNames.indexOf(columnName);
		if (idx == -1) throw new IllegalStateException("Unable to find column " + columnName + " for table " + name);
		return idx;
	}

	public String getColumnName(int columnIdx) {
		return columnNames.get(columnIdx);
	}
	
	public String getSqlSelect() {
		return "SELECT ROWID, " + String.join(",", columnNames) + " FROM " + name + " ";
	}

	public String getSqlInsert() {
		StringBuilder sb = new StringBuilder(" INSERT INTO ").append(name).append("( ");
		sb.append(String.join(",", columnNames));
		sb.append(") VALUES ( ");
		for (int i=0; i<columnNames.size(); i++) {
			if (i!=0) {
				sb.append(',');				
			}
			sb.append(" ? ");
		}
		sb.append(")");
		return sb.toString();
	}
	
	public Schema getSchema() {
		return schema;
	}

	@Override
	public String toString() {
		return "Table [name=" + name + ", columnNames=" + columnNames + ", key=" + key + "]";
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
		Table other = (Table) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	
	
	
	


}

package org.peekmoon.database.walker;

import java.sql.RowId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.peekmoon.database.walker.schema.KeyValue;
import org.peekmoon.database.walker.schema.PrimaryKey;
import org.peekmoon.database.walker.schema.Table;

public class Row {
	
	private final Table table;
	private final RowId rowId;
	private final List<Object> values;
	
	public Row(Table table, RowId rowId) {
		this.table = table;
		this.rowId = rowId;
		this.values = new ArrayList<>();
	}
	
	/**
	 * @return Values of primaryKey columns. If table have no primary key value of all columns. 
	 */
	public KeyValue getPrimaryKeyValue() {
		KeyValue keyValue = new KeyValue();
		List<Integer> columnsIdx;
		if (table.getPrimaryKey()==null) {
			columnsIdx = IntStream.range(0, values.size()).boxed().collect(Collectors.toList());
		} else {
			columnsIdx = table.getPrimaryKey().getColumnIdxs();
		}
		for (Integer idx : columnsIdx) {
			keyValue.add(values.get(idx));
		}
		return keyValue;
	}

	public void add(Object object) {
		values.add(object);
	}
	
	public List<Object> getValues() {
		return values;
	}
	
	public PrimaryKey getPrimaryKey() {
		return table.getPrimaryKey();
	}

	public Object getValue(int i) {
		return values.get(i);
	}

	public Table getTable() {
		return table;
	}
	
	public void setValue(int idcolumn, Object value){
	    values.set(idcolumn, value);
	}
	
	public void setValue(String columnName, Object value){
	    this.setValue(table.getColumnIdx(columnName), value);
	}
	
	public String values() {
		StringBuilder lineColumnName = new StringBuilder();
		StringBuilder lineValue = new StringBuilder();
		for (int i=0; i<values.size(); i++) {
			String columnName = table.getColumnName(i);
			String value = String.valueOf(values.get(i));
			int len = Math.max(columnName.length(), value.length());
			String format = "%1$-" + len + "s | ";
			lineColumnName.append(String.format(format, columnName));
			lineValue.append(String.format(format, value));
		}
		return "Row [table=" + table.getName() + ", rowid=" + rowId + "\n" + lineColumnName + "\n" + lineValue;
	}

	@Override
	public String toString() {
		return "Row [table=" + table.getName() + ", rowid=" + rowId + ", " + getPrimaryKeyValue() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((rowId == null) ? 0 : rowId.toString().hashCode()); // RowId.hashCode is corrupt
		result = prime * result + ((table == null) ? 0 : table.hashCode());
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
		Row other = (Row) obj;
		if (rowId == null) {
			if (other.rowId != null)
				return false;
		} else if (!rowId.toString().equals(other.rowId.toString()))
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}
	
	
	
	
	
	

}

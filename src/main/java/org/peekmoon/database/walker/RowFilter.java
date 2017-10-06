package org.peekmoon.database.walker;

public interface RowFilter {

	public boolean ignoreRow(Row row, Fragment fragment);

}

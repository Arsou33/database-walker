package org.peekmoon.database.walker.schema;

/**
 * An empty filter that accept all columns of all tables
 *
 */
public class EmptyFilter implements SchemaFilter {

	@Override
	public boolean ignoreColumn(String tableName, String columnName) {
		return false;
	}

	@Override
	public boolean ignoreForeignKey(String keyName) {
		return false;
	}

}

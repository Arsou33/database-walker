package org.peekmoon.database.walker;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class DatabaseTask {

	final Set<Row> ignoredRows = new HashSet<>();

	public void process(Connection conn, Fragment fragment, RowFilter filter) throws SQLException {
		if (filter != null) {
			preProcess(fragment, filter);
		}
		for (Set<Row> partition : getOrderedPartitions(fragment)) {
			for (Row row : partition) {
				postProcess(conn, fragment, row);
			}
		}
	}

	protected void preProcess(Fragment fragment, RowFilter filter) {
		fragment.getRows().stream()
			.filter(row -> filter.ignoreRow(row, fragment))
			.forEach(ignoredRows::add);
	}

	protected void postProcess(Connection conn, Fragment fragment, Row row) throws SQLException {
		if (!ignoredRows.contains(row)
				&& (fragment.getParents(row).isEmpty()
						|| fragment.getParents(row).stream().anyMatch(p -> !ignoredRows.contains(p)))) {
			process(conn, row);
		}
		else {
			ignoredRows.add(row);
		}
	}

	protected abstract void process(Connection conn, Row row) throws SQLException;

	protected abstract List<Set<Row>> getOrderedPartitions(Fragment fragment);

}

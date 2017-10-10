package org.peekmoon.database.walker;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DatabaseTask {

	final Map<Class<? extends DatabaseTask>, Set<Row>> mappedIgnoredRows = new HashMap<>();

	default void process(Connection conn, Fragment fragment, RowFilter filter) throws SQLException {
		if (filter != null) {
			preProcess(fragment, filter);
		}
		for (Set<Row> partition : getOrderedPartitions(fragment)) {
			for (Row row : partition) {
				postProcess(conn, fragment, row);
			}
		}
	}

	default void preProcess(Fragment fragment, RowFilter filter) {
		fragment.getRows().stream()
			.filter(row -> filter.ignoreRow(row, fragment))
			.forEach(row -> mappedIgnoredRows.get(this.getClass()).add(row));
	}

	default void postProcess(Connection conn, Fragment fragment, Row row) throws SQLException {
		if (!mappedIgnoredRows.get(this.getClass()).contains(row)
				&& (fragment.getParents(row).isEmpty()
						|| fragment.getParents(row).stream().anyMatch(p -> !mappedIgnoredRows.get(this.getClass()).contains(p)))) {
			process(conn, row);
		}
		else {
			mappedIgnoredRows.get(this.getClass()).add(row);
		}
	}

	void process(Connection conn, Row row) throws SQLException;

	List<Set<Row>> getOrderedPartitions(Fragment fragment);

}

package org.peekmoon.database.walker;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

abstract class DeleteOrderDatabaseTask implements DatabaseTask {

	public DeleteOrderDatabaseTask() {
		mappedIgnoredRows.computeIfAbsent(this.getClass(), c -> new HashSet<>());
	}

	@Override
	public List<Set<Row>> getOrderedPartitions(Fragment fragment) {
		return fragment.getDeleteOrderedPartitions();
	}

}

package org.peekmoon.database.walker;

import java.util.List;
import java.util.Set;

abstract class InsertOrderDatabaseTask extends DatabaseTask {

	@Override
	public List<Set<Row>> getOrderedPartitions(Fragment fragment) {
		List<Set<Row>> partitions = fragment.getInsertOrderedPartitions();
		for (Set<Row> partition : partitions) {
			if (partition.size() > 1) {
				throw new IllegalStateException("Row graph is not acyclic : " + partition);
			}
		}
		return partitions;
	}

}

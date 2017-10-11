package org.peekmoon.database.walker;

import java.util.List;
import java.util.Set;

abstract class DeleteOrderDatabaseTask extends DatabaseTask {

	@Override
	public List<Set<Row>> getOrderedPartitions(Fragment fragment) {
		return fragment.getDeleteOrderedPartitions();
	}

}

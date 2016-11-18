package org.peekmoon.database.walker.schema;

import java.util.Collections;
import java.util.List;

public class PrimaryKey extends Key {

	public PrimaryKey(String name, Table table) {
		super(name, table);
	}
	
	public List<ForeignKey> getForeignKeys() {
		return Collections.unmodifiableList(getSchema().getFkList(this));
	}

}

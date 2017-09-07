package org.peekmoon.database.walker;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

public class InsertOrderDatabaseTask implements DatabaseTask {
    
    public void process(Connection conn, Row row) throws SQLException {
        
    }

    
    public void process(Connection conn, Fragment fragment) throws SQLException {
        for (Set<Row> partition : fragment.getInsertOrderedPartitions()) {
            if (partition.size()>1) throw new IllegalStateException("Row graph is not acyclic : " + partition);
            for (Row row : partition) {
                process(conn, row);
            }
        }
    }
}

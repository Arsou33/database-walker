package org.peekmoon.database.walker;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.peekmoon.database.walker.schema.CustomBlob;
import org.peekmoon.database.walker.schema.CustomClob;
import org.peekmoon.database.walker.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseTaskConditionalInsert extends InsertOrderDatabaseTask {
    
    private final static Logger log = LoggerFactory.getLogger(DatabaseTaskDelete.class);

    @Override
    public void process(Connection conn, Row row) throws SQLException {
        if (!row.isToInsert()
        		|| (!row.getParents().isEmpty()
        				&& row.getParents().stream().allMatch(p -> !p.isToInsert()))) {
        	row.setToInsert(false);
            return;
        }
        Table table = row.getTable();
        String sql = table.getSqlInsert();
        // TODO : mutualize preparedStatement
        List<Clob> clobs = new LinkedList<>();
        List<Blob> blobs = new LinkedList<>();
        try (PreparedStatement stmt =  conn.prepareStatement(sql)) {
            log.debug(sql);

            for (int i=0; i<row.getValues().size(); i++) {
                
                Object value = row.getValue(i);
                
                if (value instanceof CustomClob) {
                    Clob clob = ((CustomClob)value).asClob(conn);
                    clobs.add(clob);
                    value = clob;
                } else if (value instanceof CustomBlob) {
                    Blob blob = ((CustomBlob)value).asBlob(conn);
                    blobs.add(blob);
                    value = blob;
                }

                stmt.setObject(i+1, value);
            }
            stmt.executeUpdate();
        } 
        finally {
            for (Clob clob : clobs) clob.free();
            for (Blob blob : blobs) blob.free();
        }
    }
}


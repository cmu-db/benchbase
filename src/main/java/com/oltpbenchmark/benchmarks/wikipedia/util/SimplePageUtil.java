package com.oltpbenchmark.benchmarks.wikipedia.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SimplePageUtil {

    public static SimplePage getSimplePage(Connection conn, int pageId) throws SQLException {

        try (PreparedStatement st = conn.prepareStatement("SELECT page_id, page_namespace, page_title FROM page where page_id = ?")) {
            st.setInt(1, pageId);
            try (ResultSet rs = st.executeQuery()) {
                if (rs.next()) {
                    return new SimplePage(rs.getInt("page_id"), rs.getInt("page_namespace"), rs.getString("page_title"));
                }
            }
        }

        return null;

    }
}

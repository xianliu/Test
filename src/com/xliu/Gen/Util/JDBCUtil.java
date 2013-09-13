package com.xliu.Gen.Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {
    public static ResultSet excuteSQL(String query) {
        List list = new ArrayList();
        try {
            Class.forName("org.postgresql.Driver");
            Connection con = DriverManager.getConnection("jdbc:postgresql://10.66.14.234:5432/edwdb", "conflux_hss",
                    "conflux_hss");
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            return rs;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}

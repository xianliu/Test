package com.xliu.Gen.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import com.xliu.Gen.Util.GenJson;
import com.xliu.Gen.Util.JDBCUtil;
import com.xliu.Gen.Util.SQLGen;

public class DateTest {

    @Test
    public void testMonthly() {
        String[] indicator = new String[0];
        String[] queue = new String[0];
        String[] groupBy = new String[] { "date" };

        String datetype = "MONTHLY";
        String dateformat = "YYYY-MM";
        String datecount = "5";
        boolean isNeedId = false;

        String query = SQLGen.getSQLByDateIndicatorQueue(indicator, queue, null, datetype, null, datecount, groupBy, isNeedId);

        System.out.println(query);

        ResultSet rs = JDBCUtil.excuteSQL(query);

        try {
            System.out.println(GenJson.getJson(rs, isNeedId, groupBy));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWeekly() {
        String[] indicator = new String[0];
        String[] queue = new String[0];
        String[] groupBy = new String[] { "date" };

        String datetype = "WEEKLY";
        String dateformat = "YYYY-ww";
        String datecount = "5";
        boolean isNeedId = false;

        String query = SQLGen.getSQLByDateIndicatorQueue(indicator, queue, null, datetype, dateformat, datecount, groupBy, isNeedId);

        System.out.println(query);

        ResultSet rs = JDBCUtil.excuteSQL(query);

        try {
            System.out.println(GenJson.getJson(rs, isNeedId, groupBy));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDaily() {
        String[] indicator = new String[0];
        String[] queue = new String[0];
        String[] groupBy = new String[] { "date" };

        String datetype = "DAILY";
        String dateformat = "YYYY-MM-dd";
        String datecount = "5";
        boolean isNeedId = false;

        String query = SQLGen.getSQLByDateIndicatorQueue(indicator, queue, null, datetype, dateformat, datecount, groupBy, isNeedId);

        System.out.println(query);

        ResultSet rs = JDBCUtil.excuteSQL(query);

        try {
            System.out.println(GenJson.getJson(rs, isNeedId, groupBy));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}

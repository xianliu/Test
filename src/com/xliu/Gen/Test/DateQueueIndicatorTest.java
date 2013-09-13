package com.xliu.Gen.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import com.xliu.Gen.Util.GenJson;
import com.xliu.Gen.Util.JDBCUtil;
import com.xliu.Gen.Util.SQLGen;

public class DateQueueIndicatorTest {

	@Test
    public void testFullValueInputMonthly() {
        String[] indicator = new String[] { "resolved open", "new open" };
        String[] queue = new String[] { "engineering-services", "engineering-services eng-ops-se", "eng-ops-se" };
        String[] groupBy = new String[] { "date",  "queue_name_group", "indicator_group" };

        String datetype = "MONTHLY";
        String dateformat = "YYYY-MM";
        String datecount = "5";
        boolean isNeedId = false;

        String query = SQLGen.getSQLByDateIndicatorQueue(indicator, queue, null, datetype, dateformat, datecount, groupBy,
                isNeedId);

        System.out.println(query);

        ResultSet rs = JDBCUtil.excuteSQL(query);

        try {
            System.out.println(GenJson.getJson(rs, isNeedId, groupBy));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFullValueInputWeekly() {
        String[] indicator = new String[] { "resolved open", "new open" };
        String[] queue = new String[] { "engineering-services", "engineering-services eng-ops-se", "eng-ops-se" };
        String[] groupBy = new String[] { "date",  "queue_name_group", "indicator_group" };

        String datetype = "WEEKLY";
        String dateformat = "YYYY-W";
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
    public void testFullValueInputDaily() {
        String[] indicator = new String[] { "resolved open", "new open" };
        String[] queue = new String[] { "engineering-services", "engineering-services eng-ops-se", "eng-ops-se" };
        String[] groupBy = new String[] { "date",  "queue_name_group", "indicator_group" };

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

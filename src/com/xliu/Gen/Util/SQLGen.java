package com.xliu.Gen.Util;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;


public class SQLGen {
    
    public static String getSQLByDateIndicatorQueue(String[] indicator, String[] queue, String datescope, String dateType,
            String dateformat, String datecount, String[] groupBy, boolean isNeedId) {

        // eg. YYYYMMDD
        dateformat = (dateformat != null) ? dateformat : getDBDateFormat(dateType);

        // default 5
        datecount = (datecount != null) ? datecount : "5";
        
        // table name
        String table_name = "\"DM\".\"" + dateType + "_TICKET_SNAPSHOT\"";

        String query = "with first as (" + "select * from (" + genTimeSlotTable(dateType, datecount)
                + " as date) timeslot left join " + genIndicatorTable(indicator, table_name) + " as indicator on 1=1 join " + genQueueTable(queue, table_name)
                + " as queue_name on 1=1)," + "second as (" + genOriginalDataTable(dateType, datecount, isNeedId) + "),"
                + "third as (" + "select first.date,first.indicator,first.queue_name,coalesce(second.count,0) as count";

        if (isNeedId) {
            query = query + ",second.ids";
        }

        query = query
                + " from first left join "
                + "second on (first.date=second.date and first.indicator=second.indicator and first.queue_name=second.queue_name)"
                + ")";

        // union the indicator group
        query = query + ",forth as (" + genUnion(indicator, "indicator", "third") + ")";

        // union the indicator group
        query = query + ",fifth as (" + genUnion(queue, "queue_name", "forth") + ")";
        
        query = query + SQLGen.genGroupByQuery(groupBy, "fifth", isNeedId, dateType, dateformat);
        
        query = query + ";";

        return query;
    }
    
    public static String genGroupByQuery(String[] groupBy, String withId, boolean isNeedId, String dateType, String targetFormat) {
        String query = " select ";
        String DBFormat = getDBDateFormat(dateType);
        
        
        for (int i = groupBy.length-1; i >=0 ; i--) {
            
            if(groupBy[i].equals("date")) {
                query = query + " to_char((to_date(" + withId + "." +groupBy[i] + ", \'" + DBFormat + "\')::timestamp), \'" + targetFormat + "\') as date"  + " ,"; 
            } else {
                query = query + withId + "." +groupBy[i] + " ,"; 
            }
            
        }
        
        query = query + " sum(count) as count";

        if (isNeedId) {
            query = query + ",array_to_string(ARRAY(SELECT unnest(array_agg(fifth.ids))),',') as ids";
        }

        String clauseQuery = "";
        
        for (int i = 0; i < groupBy.length; i++) {
            clauseQuery = clauseQuery + withId + "." + groupBy[i];
            if(i != (groupBy.length - 1)) {
                clauseQuery = clauseQuery + ", ";
            }
        }
        
        query = query + " from fifth group by ";
        
        query = query + clauseQuery;
        
        query = query + " order by ";
        
        query = query + clauseQuery;
        
        return query;
    }

    public static String genIndicatorTable(String[] indicator, String table_name) {
        String query = "";
        if(indicator.length > 0) {
            String indicator4Unnest = genGroup4Unnest(indicator);
            query = "unnest(ARRAY " + indicator4Unnest + ") ";
        } else {
            query = "(select distinct status as indicator from " + table_name + ")";
        }
        
        return query;
    }

    public static String genQueueTable(String[] queue, String table_name) {
        String query = "";
        if(queue.length > 0) {
            String queue4Unnest = genGroup4Unnest(queue);
            query = "unnest(ARRAY " + queue4Unnest + ") ";
        } else {
            query = "(select distinct queue_name from " + table_name + ")";
        }
        
        return query;
    }

    public static String genTimeSlotTable(String dateType, String datecount) {
        List<String> timeslot = generateTime(dateType, datecount);
        String query = "select unnest(ARRAY " + timeslot.toString() + ")";
        return query;
    }

    public static String genOriginalDataTable(String datetype, String datecount, boolean isNeedId) {
        // time slot
        List<String> timeslot = generateTime(datetype, datecount);
        String time4In = timeslot.toString().replaceAll("(^\\[)|(\\]$)", "");

        // datetype will decide the search table name
        datetype = (datetype != null) ? datetype.toUpperCase() : "MONTHLY";

        String table_name = "\"DM\".\"" + datetype + "_TICKET_SNAPSHOT\"";

        String query = "select snapshot as date, status as indicator,queue_name,count(*) as count";

        if (isNeedId) {
            query = query + ",array_to_string(ARRAY(SELECT unnest(array_agg(ticket_id))),',') as ids";
        }

        query = query + " from " + table_name + " group by snapshot,status,queue_name " + "having snapshot in (" + time4In
                + ") order by snapshot desc";
        return query;
    }

    public static List<String> generateTime(String dateType, String dateCount) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        int count = Integer.parseInt(dateCount);
        String format = "";
        int field = 0;
        
        format = getDBDateFormat(dateType);
        
        field = getCalendarType(dateType);
        
        List<String> result = new ArrayList<String>();

        for (int i = 0; i < count; i++) {
            Calendar currentCalendar = Calendar.getInstance();
            currentCalendar.add(field, -(i + 1));
            Date tasktime = currentCalendar.getTime();
            SimpleDateFormat df = new SimpleDateFormat(format);
            result.add("\'" + df.format(tasktime) + "\'");
        }

        return result;
    }

    public static String genGroup4Unnest(String[] indicator) {
        Set<String> indicatorSet = new HashSet<String>();

        for (int i = 0; i < indicator.length; i++) {
            String[] group = indicator[i].split(" ");
            for (int j = 0; j < group.length; j++) {
                indicatorSet.add("\'" + group[j] + "\'");
            }
        }

        return indicatorSet.toString();
    }

    public static String genUnion(String[] indicator, String field, String withId) {
        String unionQuery = "";
        if(indicator.length > 0) {
            unionQuery = "select 'start'::text as " + field + "_group,* from " + withId + " where 1=2 ";
            for (int i = 0; i < indicator.length; i++) {
                unionQuery = unionQuery + "union " + "select \'" + indicator[i] + "\'::text as " + field + "_group,* from "
                        + withId + " where 1=2 ";

                String[] innerGroup = indicator[i].split(" ");
                for (int j = 0; j < innerGroup.length; j++) {
                    unionQuery = unionQuery + "or " + withId + "." + field + "=\'" + innerGroup[j] + "\' ";
                }
            }
        } else {
            unionQuery = "select "+ field +" as " + field + "_group,* from " + withId ;
        }

        return unionQuery;
    }
    
    public static String getDBDateFormat(String dateType) {
        String format = "";
        if(dateType.equals("DAILY")) {
            format = "yyyy-MM-dd";
        }else if(dateType.equals("MONTHLY")) {
            format = "yyyy-MM";
        }else if(dateType.equals("WEEKLY")) {
            format = "yyyy-ww";
        }
        
        return format;
    }
    
    public static int getCalendarType(String dateType) {
        int field = 0;
        if (dateType.equals("DAILY")) {
            field = Calendar.DAY_OF_YEAR;
        } else if (dateType.equals("MONTHLY")) {
            field = Calendar.MONTH;
        } else if (dateType.equals("WEEKLY")) {
            field = Calendar.WEEK_OF_YEAR;
        }
        
        return field;
    }

}
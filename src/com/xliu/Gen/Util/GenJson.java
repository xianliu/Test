package com.xliu.Gen.Util;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.gson.Gson;


public class GenJson {
    
    public static String getJson(ResultSet rs, boolean isNeedId, String[] groupBy) throws SQLException {
        Map<String, Object> resultMap = GenJson.resultSetToMap(rs);
        List<Map<String, Set<String>>> groupList = GenJson.getGroupList(resultMap);
        
        int leverage = groupList.size();
        int num = leverage; 
        Map result = GenJson.format(num, leverage, resultMap, "", "", "", isNeedId);
        
		Map highchart = GenJson.genHighCHart(resultMap , groupBy);
        
		System.out.println(highchart);
		
		result.put("highchart", highchart);
		
        Gson gson = new Gson();
        return gson.toJson(result);
    }
	
    public static Map<String, Object> format(int num, int leverage, Map<String, Object> resultMap, String path,
            String preGroupName, String preGroupValue, boolean isNeedId) throws SQLException {
		Map<String, Object> outerMap = new LinkedHashMap<String, Object>();
		List subList = new LinkedList();
		
		List<Map<String, Set<String>>> groupList = GenJson.getGroupList(resultMap);
		
		Map<String, Set<String>> groupMap = groupList.get(num - 1);
		Set<String> groupValues = new LinkedHashSet<String>();
        String groupName = new String();
        
        for (Entry<String, Set<String>> mapEntry : groupMap.entrySet()) {
            groupName = mapEntry.getKey();
            groupValues = mapEntry.getValue();
        }
        
		if(num == 1) {
		    for(String groupValue : groupValues) {
		        Map<String, Object> map = new LinkedHashMap<String, Object>();
		        String objectPath = path + groupValue;
                map.put(groupName, groupValue);
                map.put("count", getValue(objectPath, resultMap, "count"));
                if(isNeedId) {
                    map.put("ids", getValue(objectPath, resultMap, "ids"));
                }
                subList.add(map);
		    }
		    
		    if(leverage == 1) {
                outerMap.put("result", subList);
		    } else {
		        outerMap.put(preGroupName, preGroupValue);
	            outerMap.put("Objects", subList);
		    }
		
		} else {
			for (String groupValue : groupValues) {
			    String objectPath = path + groupValue + ","; 
				subList.add(format(num-1,leverage, resultMap, objectPath, groupName, groupValue, isNeedId));
			}
			
			if(num == leverage) {
				outerMap.put("result", subList);
			} else {
				outerMap.put(preGroupName, preGroupValue);
				outerMap.put("Objects", subList);
			}
		}
		
		return outerMap;
	}
	
	//group order used to generate json structure
	public static List<String> getGroupOrder(Map<String, Object> resultMap) throws SQLException {
	    Set<String> originalSet = (Set<String>) resultMap.get("column");
	    originalSet.remove("count");
	    List<String> originalList = new LinkedList();
	    originalList.addAll(originalSet);
	    List list = new LinkedList();
	    
	    //inverse the original list(Recursion causes the order inverse)
        for (int i = originalList.size() - 1; i >= 0; i--) {
            list.add(originalList.get(i));
        }
	   
        return list;
	}
	
	public static List<Map<String, Set<String>>> getGroupList(Map<String, Object> resultMap) throws SQLException {
	    List resultList = new ArrayList();
	    Set<String> columnSet = (Set<String>) resultMap.get("column");
	    List tablesValue = (List) resultMap.get("table_value");
	    
	    if (resultMap == null)
            return Collections.EMPTY_LIST;
	    

        List<Map<String, Set<String>>> list = new LinkedList();
        Map<String, Set<String>> rowData = new LinkedHashMap<String, Set<String>>();
        

        for (int i = 0; i < tablesValue.size(); i++) {
            Map record = (Map) tablesValue.get(i);
            
            for (String columnName : columnSet) {
                if((!columnName.equals("count"))&& (!columnName.equals("ids"))) {
                    if(rowData.get(columnName) == null) {
                        Set set = new LinkedHashSet();
                        set.add(record.get(columnName).toString());
                        rowData.put(columnName, set);
                    } else {
                        Set set = (Set) rowData.get(columnName);
                        set.add(record.get(columnName).toString());
                        rowData.put(columnName, set);
                    }
                }
                
            }
            
        }
        
        for (Entry<String, Set<String>> entry : rowData.entrySet()) {
            Map<String, Set<String>> singleMap = new HashMap<String, Set<String>>();
            singleMap.put(entry.getKey(), entry.getValue());
            list.add(singleMap);
        }
        
	    return list;
	}
	
    public static String getValue(String path, Map<String, Object> resultMap, String field) throws SQLException {
        List<Map<String, Object>> tableList = (List<Map<String, Object>>) resultMap.get("table_value");
        List<String> orderList = getGroupOrder(resultMap);
        String[] groups = path.split(",");

        for (int j = 0; j < tableList.size(); j++) {
            Map<String, Object> record = tableList.get(j);
            boolean flag = true;
            
            for (int i = 0; i < groups.length; i++) {
                if(!record.get(orderList.get(i)).equals(groups[i])) {
                    flag = false;
                }
            }
            
            if (flag) { 
                return record.get(field).toString();
            }

        }

        return "";
    }
    
    public static Map<String, Object> resultSetToMap(ResultSet rs) throws java.sql.SQLException {
        Map<String, Object> map = new HashMap<String, Object>();
        Set<String> columnList = new LinkedHashSet<String>();
        
        if (rs == null)
            return Collections.EMPTY_MAP;
        
        ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();
        List list = new LinkedList();
        Map rowData = new LinkedHashMap();
        while (rs.next()) {
            rowData = new LinkedHashMap(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                columnList.add(md.getColumnName(i));
                rowData.put(md.getColumnName(i), rs.getObject(i));
            }
            list.add(rowData);
        }
        
        map.put("table_value", list);
        
        map.put("column", columnList);
    
        return map;
    }
    
    public static Map genHighCHart(Map<String, Object> resultMap, String[] groupBy) {
    	List table_value = (List) resultMap.get("table_value");
    	
    	Map returnMap = new LinkedHashMap();
    	
    	Map<String, Set> groupMap = new LinkedHashMap<String, Set>();
    	
    	Set<String> catrgories = new LinkedHashSet<String>();
    	
    	List<Object> series = new LinkedList<Object>();
    	
    	for (int i = 0; i < table_value.size(); i++) {
			Map record = (Map) table_value.get(i);
			
			for (int j = 0; j < groupBy.length; j++) {
				Set set = new LinkedHashSet();
				String key = groupBy[j];
				String value = (String) record.get(groupBy[j]);
				
				Set<String> groupSet = new LinkedHashSet<String>();
				
				if(groupMap.get(key) != null) {
					groupSet = groupMap.get(key);
				}
				
				groupSet.add(value);
				groupMap.put(key, groupSet);
			}
    	}
    	
    	catrgories = groupMap.get(groupBy[0]);
    	
    	Map map = new LinkedHashMap();
    	List dataList = new LinkedList();
    	
		switch (groupBy.length) {
		case 1:
			map = new LinkedHashMap();
    		dataList = new LinkedList();
    		for (int i = 0; i < table_value.size(); i++) {
    			Map record = (Map) table_value.get(i);
    			String value = record.get("count").toString();
    			dataList.add(value);
    		}
    		
    		map.put("data", dataList);
    		series.add(map);
			break;

		case 2:
			for(Object o1 : groupMap.get(groupBy[1])) {
				map = new LinkedHashMap();
				map.put("name", o1.toString());
				List<String> conditionList = new LinkedList<String>();
				conditionList.add(o1.toString());
				dataList = getSeriesData(conditionList, resultMap, groupBy);
				map.put("data", dataList);
				series.add(map);
        	}
			break;

		case 3:
			for(Object o1 : groupMap.get(groupBy[1])) {
        		for(Object o2 : groupMap.get(groupBy[2])) {
        			map = new LinkedHashMap();
        			map.put("stack", o1.toString());
    				map.put("name",  o2.toString());
    				
    				List<String> conditionList = new LinkedList<String>();
    				conditionList.add(o1.toString());
    				conditionList.add(o2.toString());
    				
    				dataList = getSeriesData(conditionList, resultMap, groupBy);
    				
    				map.put("data", dataList);
    				
    				series.add(map);
        		}
        	}
			break;
		}
    	
    	returnMap.put("categories", catrgories);
    	returnMap.put("series", series);
    	
    	return returnMap;
    }
    
    public static List getSeriesData(List<String> conditionList, Map<String, Object> resultMap, String[] groupBy) {
    	List table_value = (List) resultMap.get("table_value");
    	
    	List data = new LinkedList();
    	
    	for (int i = 0; i < table_value.size(); i++) {
    		Map record = (Map) table_value.get(i);
    		
    		boolean flag = true;
    		
    		for (int j = 0; j < conditionList.size(); j++) {
				if(!record.get(groupBy[j+1]).equals(conditionList.get(j))) {
					flag = false;
				}
			}
    		
    		if(flag) {
    			data.add(record.get("count"));
    		}
    		
		}
    	
    	return data;
    }
  
}

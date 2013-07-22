package etu.splunk.hbase;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import etu.util.config.Constants;

public class Csv4Splunk {

	static Configuration conf = HBaseConfiguration.create();
	private static final Logger logger = Logger.getLogger(Csv4Splunk.class);
	static ObjectMapper mapper = new ObjectMapper();
	static {
		conf.set("hbase.zookeeper.quorum", Constants.ZOOKEEPER_QUORUM);
		conf.set("hbase.zookeeper.property.clientPort",
				Constants.ZOOKEEPER_CLIENT_PORT);
	}

	private static void print(ResultScanner rs){
		List<List<String>> results = new ArrayList<List<String>>();
		List<String> keys	=	new ArrayList<String>();
		boolean flag	=	true;
		for (Result r : rs) {
			String key = "";
			List<String> result	=	new ArrayList<String>();
			for (KeyValue kv : r.raw()) {
				if(flag){
					key = new StringBuffer()
						.append(Bytes.toString(kv.getFamily()))
						.append(Constants.COLUMN_SPLIT)
						.append(Bytes.toString(kv.getQualifier()))
						.toString();
					keys.add(key);
				}
				result.add(Bytes.toString(kv.getValue()));
			}
			if(flag){
				results.add(keys);
				flag=false;
			}
			results.add(result);
			printList(results);
		}
		output(results);
	}
	private static void print(Result[] rs){
		List<List<String>> results = new ArrayList<List<String>>();
		List<String> keys	=	new ArrayList<String>();
		boolean flag	=	true;
		for (Result r : rs) {
			String key = "";
			List<String> result	=	new ArrayList<String>();
			for (KeyValue kv : r.raw()) {
				if(flag){
					key = new StringBuffer()
					.append(Bytes.toString(kv.getFamily()))
					.append(Constants.COLUMN_SPLIT)
					.append(Bytes.toString(kv.getQualifier()))
					.toString();
					keys.add(key);
				}
				result.add(Bytes.toString(kv.getValue()));
			}
			if(flag){
				results.add(keys);
				flag=false;
			}
			results.add(result);
			printList(results);
		}
		output(results);
	}
	
	public static void selectList(String tablename, List<String> rks) {
		try {
			HTable table = new HTable(conf, tablename);
			List<Get> gs = new ArrayList<Get>();
			for (String key : rks) {
				Get g = new Get(Bytes.toBytes(key));
				gs.add(g);
			}
			Result[] rs = table.get(gs);
			print(rs);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	public static void selectByRowKey(String tablename, String rowKey) {
		selectByRowKey(tablename, rowKey, null);
	}

	public static void selectByRowKey(String tablename, String rowKey,
			List<String> columns) {
		List<List<String>> results = new ArrayList<List<String>>();

		try {
			HTable table = new HTable(conf, tablename);
			Get g = new Get(Bytes.toBytes(rowKey));

			if (columns != null) {
				for (String str : columns) {
					String[] keys = str.split(Constants.COLUMN_SPLIT);
					if (keys.length > 1) {
						g.addColumn(Bytes.toBytes(keys[0]),
								Bytes.toBytes(keys[1]));
					} else {
						g.addFamily(Bytes.toBytes(keys[0]));
					}
				}
			}
			Result rs = table.get(g);
			List<String> keys	=	new ArrayList<String>();
			String key = "";
			List<String> result	=	new ArrayList<String>();
			for (KeyValue kv : rs.raw()) {
				key = new StringBuffer().append(Bytes.toString(kv.getFamily()))
						.append(Constants.COLUMN_SPLIT)
						.append(Bytes.toString(kv.getQualifier())).toString();
				keys.add(key);
				result.add(Bytes.toString(kv.getValue()));
			}
			
			results.add(keys);
			results.add(result);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		output(results);
	}

	public static void scan(String tablename) {
		scan(tablename, 100);
	}

	public static void scan(String tablename, int limit) {
		List<List<String>> results = new ArrayList<List<String>>();
		try {
			HTable table = new HTable(conf, tablename);
			Scan s = new Scan();
			ResultScanner rs = table.getScanner(s);
			
			List<String> keys	=	new ArrayList<String>();
			boolean flag	=	true;
			for (Result r : rs) {
				String key = "";
				List<String> result	=	new ArrayList<String>();
				for (KeyValue kv : r.raw()) {
					if(flag){
						key = new StringBuffer()
							.append(Bytes.toString(kv.getFamily()))
							.append(Constants.COLUMN_SPLIT)
							.append(Bytes.toString(kv.getQualifier()))
							.toString();
						keys.add(key);
					}
					result.add(Bytes.toString(kv.getValue()));
				}
				if(flag){
					results.add(keys);
					flag=false;
				}
				results.add(result);
				if (results.size() >= limit) {
					output(results);
					rs.close();
					return;
				}
			}
			
			rs.close();
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		output(results);
	}

	public static void scanRange(String tablename, String startRow,
			String endRow) {
		scanRange(tablename, startRow, endRow, null);
	}

	public static void scanRange(String tablename, String startRow,
			String endRow, List<String> columns) {
		List<Map<String, String>> results = new ArrayList<Map<String, String>>();
		try {
			HTable table = new HTable(conf, tablename);
			Scan s = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
			if (columns != null) {
				for (String str : columns) {
					String[] keys = str.split(Constants.COLUMN_SPLIT);
					if (keys.length > 1) {
						s.addColumn(Bytes.toBytes(keys[0]),
								Bytes.toBytes(keys[1]));
					} else {
						s.addFamily(Bytes.toBytes(keys[0]));
					}
				}
			}
			ResultScanner rs = table.getScanner(s);
			print(rs);
			rs.close();
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		output(results);
	}

	public static void scanRangeByFilter(String tablename, String startRow,
			String endRow, String filters, List<String> columns) {
		String[] arr = filters.split(Constants.FILTER_SPLIT);
		scanRangeByFilter(tablename, startRow, endRow, Arrays.asList(arr),
				columns);
	}

	public static void scanRangeByFilter(String tablename, String startRow,
			String endRow, List<String> arr) {
		scanRangeByFilter(tablename, startRow, endRow, arr, null);
	}

	public static void scanRangeByFilter(String tablename, String startRow,
			String endRow, List<String> arr, List<String> columns) {
		List<Map<String, String>> results = new ArrayList<Map<String, String>>();
		try {
			HTable table = new HTable(conf, tablename);
			Scan s = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
			if (columns != null) {
				for (String str : columns) {
					String[] keys = str.split(Constants.COLUMN_SPLIT);
					if (keys.length > 1) {
						s.addColumn(Bytes.toBytes(keys[0]),
								Bytes.toBytes(keys[1]));
					} else {
						s.addFamily(Bytes.toBytes(keys[0]));
					}
				}
			}
			FilterList filterList = new FilterList();
			for (String v : arr) { // 各个条件之间是&&的关系
				String[] str = v.split(Constants.PARAM_SPLIT);
				filterList.addFilter(new SingleColumnValueFilter(Bytes
						.toBytes(str[0]), Bytes.toBytes(str[1]),
						CompareOp.EQUAL, Bytes.toBytes(str[2])));
			}
			s.setFilter(filterList);
			ResultScanner rs = table.getScanner(s);
			print(rs);
			rs.close();
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		output(results);
	}

	public static void output(Object o) {
		StringWriter writer = new StringWriter();
		try {
			mapper.writeValue(writer, o);
			System.out.println(writer.toString());
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public static void printList(List<List<String>> list) {
		if (list.size() < Constants.BATCH_SIZE) {
			return;
		}
		output(list);
		list.clear();
	}

	public static void main(String[] args) {
		String cmd = args[0];
		List<String> cols = null;
		int index = 0;

		if (cmd.equals(Constants.CMD_SCAN)) {
			int limit = 100;
			if (args.length > 2) {
				limit = Integer.parseInt(args[2]);
			}
			scan(args[1], limit);
		} else if (cmd.equals(Constants.CMD_SELECT_ROW)) {
			index = 3;
			cols = parseColumns(args, index);
			selectByRowKey(args[1], args[2], cols);
		} else if (cmd.equals(Constants.CMD_SELECT_LIST)) {
			index = 2;
			cols = parseColumns(args, index);
			selectList(args[1],cols);
		} else if (cmd.equals(Constants.CMD_SCAN_RANGE)) {
			index = 4;
			cols = parseColumns(args, index);
			scanRange(args[1], args[2], args[3], cols);
		} else if (cmd.equals(Constants.CMD_SCAN_RANGE_FILTER)) {
			index = 5;
			cols = parseColumns(args, index);
			scanRangeByFilter(args[1], args[2], args[3], args[4], cols);
		}
	}

	private static List<String> parseColumns(String[] args, int flag) {
		if (args.length > flag) {
			List<String> cols = new ArrayList<String>();
			for (int i = flag; i < args.length; i++) {
				cols.add(args[i]);
			}
			return cols;
		} else {
			return null;
		}
	}
}

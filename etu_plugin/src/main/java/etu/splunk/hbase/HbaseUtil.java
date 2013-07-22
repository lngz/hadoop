package etu.splunk.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import etu.util.config.Constants;

public class HbaseUtil {

	static Configuration conf = HBaseConfiguration.create();
	private static final Logger logger = Logger.getLogger(HbaseUtil.class);
	static {
		conf.set("hbase.zookeeper.quorum", Constants.ZOOKEEPER_QUORUM);
		conf.set("hbase.zookeeper.property.clientPort",
				Constants.ZOOKEEPER_CLIENT_PORT);
	}

	/**
	 * create table
	 */
	public static boolean createTable(String tablename, String[] columns) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tablename)) {
				logger.info("table : "+tablename+" Exists!!!");
				return false;
			} else {
				HTableDescriptor tableDesc = new HTableDescriptor(tablename);
				for (String column : columns) {
					tableDesc.addFamily(new HColumnDescriptor(column));
				}
				admin.createTable(tableDesc);
				logger.info("create table ok.");
				return true;
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return false;
		}
	}

	public static void dropTable(String tablename) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tablename)) {
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
				logger.info("drop table ok.");
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public static HTable getTable(String tablename) throws IOException {
		HTable table = new HTable(conf, tablename);
		table.setAutoFlush(false);
		// table.setAutoFlush(true);
		table.setWriteBufferSize(1024 * 1024 * 10);
		return table;
	}

	public static void closeTable(HTable table) throws IOException {
		table.flushCommits();
		table.close();
	}


	/**
	 * get方式，通过rowKey、column查询
	 * 
	 * @param tablename
	 * @param rowKey
	 * @param column
	 * @throws IOException
	 */
	public static Map<String, String> selectByRowKeyColumn(String tablename,
			String rowKey, String column) {
		Map<String, String> result = new HashMap<String, String>();
		try {
			HTable table = new HTable(conf, tablename);
			Get g = new Get(Bytes.toBytes(rowKey));
			g.addFamily(Bytes.toBytes(column));
			Result rs = table.get(g);
			String key = "";
			for (KeyValue kv : rs.raw()) {
				key = new StringBuffer().append(Bytes.toString(kv.getFamily())).append(":")
						.append(Bytes.toString(kv.getQualifier())).toString();
				result.put(key, new String(kv.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	
	public static List<Map<String, String>> scanerRange(String tablename,
			String startRow, String endRow) {
		List<Map<String, String>> results = new ArrayList<Map<String, String>>();
		try {
			HTable table = new HTable(conf, tablename);
			Scan s = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				Map<String, String> m = new HashMap<String, String>();
				String key = "";
				for (KeyValue kv : r.raw()) {
					key = new StringBuffer().append(Bytes.toString(kv.getFamily())).append(":")
							.append(Bytes.toString(kv.getQualifier())).toString();
					m.put(key, new String(kv.getValue()));
				}
				results.add(m);
			}
			rs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return results;
	}
	public static List<Map<String, String>> scanerRangeByFilter(String tablename,
			String startRow, String endRow,List<String> arr) {
		List<Map<String, String>> results = new ArrayList<Map<String, String>>();
		try {
			HTable table = new HTable(conf, tablename);
			Scan s = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
			FilterList filterList = new FilterList();
			for (String v : arr) { // 各个条件之间是&&的关系
				String[] str = v.split(",");
				filterList.addFilter(new SingleColumnValueFilter(Bytes
						.toBytes(str[0]), Bytes.toBytes(str[1]), CompareOp.EQUAL, Bytes
						.toBytes(str[2])));
			}
			s.setFilter(filterList);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				Map<String, String> m = new HashMap<String, String>();
				String key = "";
				for (KeyValue kv : r.raw()) {
					key = new StringBuffer().append(Bytes.toString(kv.getFamily())).append(":")
					.append(Bytes.toString(kv.getQualifier())).toString();
					m.put(key, new String(kv.getValue()));
				}
				results.add(m);
			}
			rs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return results;
	}

	/**
	 * 显示所有数据
	 * 
	 * @throws IOException
	 */
	public static List<Map<String, String>> scan(String tablename) {
		List<Map<String, String>> results = new ArrayList<Map<String, String>>();
		try {
			HTable table = new HTable(conf, tablename);
			Scan s = new Scan();
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				Map<String, String> m = new HashMap<String, String>();
				String key = "";
				for (KeyValue kv : r.raw()) {
					key = new StringBuffer().append(Bytes.toString(kv.getFamily())).append(":")
							.append(Bytes.toString(kv.getQualifier())).toString();
					m.put(key, new String(kv.getValue()));
				}
				results.add(m);
			}
			rs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return results;
	}
	
	public static List<Map<String, byte[]>> scan(HTable table) {
		List<Map<String, byte[]>> results = new ArrayList<Map<String, byte[]>>();
		try {
			Scan s = new Scan();
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				Map<String, byte[]> m = new HashMap<String, byte[]>();
				String key = "";
				for (KeyValue kv : r.raw()) {
					key = new StringBuffer().append(Bytes.toString(kv.getFamily())).append(":")
					.append(Bytes.toString(kv.getQualifier())).toString();
					m.put(key, kv.getValue());
				}
				results.add(m);
			}
			rs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return results;
	}

	public static List<Map<String, String>> scanByFilter(String tablename,
			List<String> arr) throws IOException {
		List<Map<String, String>> results = new ArrayList<Map<String, String>>();
		HTable table = new HTable(conf, tablename);
		FilterList filterList = new FilterList();
		Scan s1 = new Scan();
		for (String v : arr) { // 各个条件之间是&&的关系
			String[] s = v.split(Constants.SPLIT);
			filterList.addFilter(new SingleColumnValueFilter(Bytes
					.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL, Bytes
					.toBytes(s[2])));
		}
		s1.setFilter(filterList);
		ResultScanner ResultScannerFilterList = table.getScanner(s1);
		for (Result rr = ResultScannerFilterList.next(); rr != null; rr = ResultScannerFilterList
				.next()) {
			Map<String, String> m = new HashMap<String, String>();
			String key = "";
			for (KeyValue kv : rr.raw()) {
				key = new StringBuffer().append(Bytes.toString(kv.getFamily())).append(":")
						.append(Bytes.toString(kv.getQualifier())).toString();
				m.put(key, new String(kv.getValue()));
			}
			results.add(m);
		}
		return results;
	}
	
	
	
	public static Map<String, String> selectByRowKey(String tablename, String rowKey) {
		return selectByRowKey(tablename, rowKey, null);
	}

	/**
	 * get方式，通过rowKey、Family查询
	 * 
	 * @param tablename
	 * @param rowKey
	 * @param column
	 */
	public static Map<String, String> selectByRowKey(String tablename, String rowKey,
			List<String> columns) {
		Map<String, String> result = new HashMap<String, String>();
		try {
			HTable table = new HTable(conf, tablename);
			Get g = new Get(Bytes.toBytes(rowKey));

			if (columns != null) {
				for (String str : columns) {
					String[] keys = str.split(Constants.COLUMN_SPLIT);
					g.addFamily(Bytes.toBytes(keys[0]));
					if (keys.length > 1) {
						g.addColumn(Bytes.toBytes(keys[0]),
								Bytes.toBytes(keys[1]));
					}
				}
			}
			Result rs = table.get(g);
			String key = "";
			for (KeyValue kv : rs.raw()) {
				key = new StringBuffer().append(Bytes.toString(kv.getFamily()))
						.append(Constants.COLUMN_SPLIT)
						.append(Bytes.toString(kv.getQualifier())).toString();
				result.put(key, new String(kv.getValue()));
			}
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		return result;
	}

	public static List<Map<String, String>> scanRange(String tablename, String startRow,
			String endRow) {
		return scanRange(tablename, startRow, endRow, null);
	}

	public static List<Map<String, String>> scanRange(String tablename, String startRow,
			String endRow, List<String> columns) {
		List<Map<String, String>> results = new ArrayList<Map<String, String>>();
		try {
			HTable table = new HTable(conf, tablename);
			Scan s = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
			if (columns != null) {
				for (String str : columns) {
					String[] keys = str.split(Constants.COLUMN_SPLIT);
					s.addFamily(Bytes.toBytes(keys[0]));
					if (keys.length > 1) {
						s.addColumn(Bytes.toBytes(keys[0]),
								Bytes.toBytes(keys[1]));
					}
				}
			}
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				Map<String, String> m = new HashMap<String, String>();
				String key = "";
				for (KeyValue kv : r.raw()) {
					key = new StringBuffer()
							.append(Bytes.toString(kv.getFamily()))
							.append(Constants.COLUMN_SPLIT)
							.append(Bytes.toString(kv.getQualifier()))
							.toString();
					m.put(key, new String(kv.getValue()));
				}
				results.add(m);
			}
			rs.close();
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		return results;
	}
	
	public static List<Map<String, String>> scanRangeByFilter(String tablename, String startRow,
			String endRow, String filters, List<String> columns) {
		String[] arr = filters.split(Constants.FILTER_SPLIT);
		return scanRangeByFilter(tablename, startRow, endRow, Arrays.asList(arr), columns);
	}

	public static List<Map<String, String>> scanRangeByFilter(String tablename, String startRow,
			String endRow, List<String> arr) {
		return scanRangeByFilter(tablename, startRow, endRow, arr, null);
	}

	public static List<Map<String, String>> scanRangeByFilter(String tablename, String startRow,
			String endRow, List<String> arr, List<String> columns) {
		List<Map<String, String>> results = new ArrayList<Map<String, String>>();
		try {
			HTable table = new HTable(conf, tablename);
			Scan s = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
			if (columns != null) {
				for (String str : columns) {
					String[] keys = str.split(Constants.COLUMN_SPLIT);
					s.addFamily(Bytes.toBytes(keys[0]));
					if (keys.length > 1) {
						s.addColumn(Bytes.toBytes(keys[0]),
								Bytes.toBytes(keys[1]));
					}
				}
			}
			FilterList filterList = new FilterList();
			for (String v : arr) { // 各个条件之间是&&的关系
				String[] str = v.split(Constants.SPLIT);
				filterList.addFilter(new SingleColumnValueFilter(Bytes
						.toBytes(str[0]), Bytes.toBytes(str[1]),
						CompareOp.EQUAL, Bytes.toBytes(str[2])));
			}
			s.setFilter(filterList);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				Map<String, String> m = new HashMap<String, String>();
				String key = "";
				for (KeyValue kv : r.raw()) {
					key = new StringBuffer()
							.append(Bytes.toString(kv.getFamily()))
							.append(Constants.COLUMN_SPLIT)
							.append(Bytes.toString(kv.getQualifier()))
							.toString();
					m.put(key, new String(kv.getValue()));
				}
				results.add(m);
			}
			rs.close();
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		return results;
	}

}

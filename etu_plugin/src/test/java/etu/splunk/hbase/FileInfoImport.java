package etu.splunk.hbase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class FileInfoImport {
	
	private static String filepath	=	"d:/test/pic/";
	private static String tabName	=	"fileT";
	
	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		
		try {
//			HbaseUtil.dropTable(tabName);
			HbaseUtil.createTable(tabName, new String [] {"info","content"});
			HTable table = HbaseUtil.getTable(tabName);
			List<Put> lp = new ArrayList<Put>();
			File file = new File(filepath);
			File[] files = file.listFiles();
			if (files.length == 0)
				return;
			for (File f : files) {// s 为文件名
				if (f.isDirectory()) {
					continue;
				}
				Put put = getFilePut(f);
				if(put!=null)
					lp.add(put);
			}
			table.put(lp);
			HbaseUtil.closeTable(table);
		} catch (Exception e) {
			e.printStackTrace();
		}
		long cost = System.currentTimeMillis() - start;
		System.out.println("cost time : " + cost + " (s)");
	}
	
	public static Put getFilePut(File file){
		String filename	=	file.getName();
		String type		=	filename.substring(filename.indexOf(".")+1);
		Put put = new Put(Bytes.toBytes(file.getName()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("filename"),
				Bytes.toBytes(filename));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("filesize"),
				Bytes.toBytes(file.length()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("type"),
				Bytes.toBytes(type));
		try {
			put.add(Bytes.toBytes("content"), Bytes.toBytes("content"),
					FileUtils.readFileToByteArray(file));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return put;
	}
	
}

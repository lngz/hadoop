package etu.splunk.hbase;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class FileInfoExport {

	private static String destPath	=	"d:/test/ret/";
	
	public static void main(String[] args) {
		try {
			HTable table = HbaseUtil.getTable("fileT");
			List<Map<String, byte[]>> list = HbaseUtil.scan(table);
			for (Map<String, byte[]> m : list) {
				String filename = destPath + Bytes.toString(m.get("info:filename"));
				try {
					FileUtils.writeByteArrayToFile(new File(filename),
							m.get("content:content"));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			HbaseUtil.closeTable(table);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

package etu.splunk.hbase;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PVImport {

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(
					PVImport.class.getResourceAsStream("/pv.txt")));
			String line = null;
			CommonModel model = null;
			long count = 0;
			HTable table = HbaseUtil.getTable("pv");
			List<Put> lp = new ArrayList<Put>();
			while ((line = reader.readLine()) != null) {
				model = new CommonModel(line);
				Put put = getPut(model);
				lp.add(put);
				count++;
				if (count % 5000 == 0) {
					table.put(lp);
					lp.clear();
					System.out.println("add data count : " + count);
				}
			}
			table.put(lp);
			HbaseUtil.closeTable(table);
			long cost = System.currentTimeMillis() - start;
			System.out.println("cost time : " + cost + " (s)");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
	}

	private static Put getPut(CommonModel model) {
		if (model == null)
			return null;
		Put put = new Put(Bytes.toBytes(model.getKey()));
		put.add(Bytes.toBytes("user"), Bytes.toBytes("uid"),
				Bytes.toBytes(model.getUserId()));
		put.add(Bytes.toBytes("user"), Bytes.toBytes("cid"),
				Bytes.toBytes(model.getChannelId()));
		put.add(Bytes.toBytes("ipinfo"), Bytes.toBytes("country"),
				Bytes.toBytes(model.getCountry()));
		put.add(Bytes.toBytes("ipinfo"), Bytes.toBytes("province"),
				Bytes.toBytes(model.getProvince()));
		put.add(Bytes.toBytes("ipinfo"), Bytes.toBytes("isp"),
				Bytes.toBytes(model.getIsp()));
		put.add(Bytes.toBytes("state"), Bytes.toBytes("httpcode"),
				Bytes.toBytes(model.getHttpCode()));
		put.add(Bytes.toBytes("state"), Bytes.toBytes("hit"),
				Bytes.toBytes(model.getHitStatus()));
		put.add(Bytes.toBytes("state"), Bytes.toBytes("timeStr"),
				Bytes.toBytes(model.getEndTime()));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("pv"),
				Bytes.toBytes(model.getCountPV() + ""));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("req"),
				Bytes.toBytes(model.getRequestCount() + ""));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("flux"),
				Bytes.toBytes(model.getTraffic() + ""));
		put.setWriteToWAL(false); // 禁用hbase的预写日志功能（WAL） --提升效率
		return put;
	}
}

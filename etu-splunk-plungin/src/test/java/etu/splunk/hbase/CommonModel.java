package etu.splunk.hbase;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CommonModel {
	private String userId;
	private String channelId;
	private String endTime;
	private String isp;
	private String province;
	private String country;
	private String httpCode;
	private String hitStatus;
	private long traffic = 0;
	private long countPV = 0; // PV数
	private long requestCount = 0; // 请求数

	public CommonModel() {

	}

	public CommonModel(String line) {
		String[] splits = line.split(",");
		this.userId = splits[0];
		this.channelId = splits[1];
		this.endTime = parseTime(splits[2]);
		this.isp = splits[3];
		this.province = splits[4];
		this.httpCode = splits[5];
		this.hitStatus = splits[6];
		this.countPV = Long.parseLong(splits[7]);
		this.traffic = Long.parseLong(splits[8]);
		this.country = splits[9];
		this.requestCount = Long.parseLong(splits[10]);
	}

	public void setKeyValues(String keyLine) {
		String[] splits = keyLine.split("_");
		this.userId = splits[0];
		this.channelId = splits[1];
		this.country = splits[2];
		this.isp = splits[3];
		this.province = splits[4];
		this.httpCode = splits[5];
		this.hitStatus = splits[6];
		this.endTime = splits[7];
	}
	
	private String parseTime(String timeStr) {
		SimpleDateFormat format		=	new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat format_day	=	new SimpleDateFormat("yyyyMMddHH");
		String ret	=	"";
		try {
			Date date = format.parse(timeStr);
			ret = format_day.format(date);
		} catch (Exception e) {
		}
		return ret;
	}

	public void setValues(String valueLine) {
		String[] splits = valueLine.split("_");
		this.countPV = Long.parseLong(splits[0]);
		this.requestCount = Long.parseLong(splits[1]);
		this.traffic = Long.parseLong(splits[2]);
	}

//	public String getKey() {
//		StringBuffer sb = new StringBuffer();
//		sb.append(this.getUserId()).append("_").append(this.getChannelId())
//				.append("_").append(this.getEndTime()).append("_")
//				.append(this.getIsp()).append("_").append(this.getProvince())
//				.append("_").append(this.getHttpCode()).append("_")
//				.append(this.getHitStatus()).append("_")
//				.append(this.getCountry());
//		return sb.toString();
//	}
	
	public String getKey() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.getUserId())
				.append("_").append(this.getChannelId())
				.append("_").append(this.getCountry())
				.append("_").append(this.getIsp())
				.append("_").append(this.getProvince())
				.append("_").append(this.getHttpCode())
				.append("_").append(this.getHitStatus())
				.append("_").append(this.getEndTime())
				;
		return sb.toString();
	}

	public String getValue() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.getCountPV()).append("_").append(this.getRequestCount())
				.append("_").append(this.getTraffic());
		return sb.toString();
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getChannelId() {
		return channelId;
	}

	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public String getIsp() {
		return isp;
	}

	public void setIsp(String isp) {
		this.isp = isp;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getHttpCode() {
		return httpCode;
	}

	public void setHttpCode(String httpCode) {
		this.httpCode = httpCode;
	}

	public String getHitStatus() {
		return hitStatus;
	}

	public void setHitStatus(String hitStatus) {
		this.hitStatus = hitStatus;
	}

	public long getTraffic() {
		return traffic;
	}

	public void setTraffic(long traffic) {
		this.traffic = traffic;
	}

	public long getCountPV() {
		return countPV;
	}

	public void setCountPV(long countPV) {
		this.countPV = countPV;
	}

	public long getRequestCount() {
		return requestCount;
	}

	public void setRequestCount(long requestCount) {
		this.requestCount = requestCount;
	}

	public void merge(CommonModel o) {
		this.countPV += o.getCountPV();
		this.requestCount += o.getRequestCount();
		this.traffic += o.getTraffic();
	}

	public void merge(String valueLine) {
		String[] splits = valueLine.split("_");
		long countPV = Long.parseLong(splits[0]);
		long requestCount = Long.parseLong(splits[1]);
		long traffic = Long.parseLong(splits[2]);

		this.countPV += countPV;
		this.requestCount += requestCount;
		this.traffic += traffic;
	}

	public static void main(String[] args) {
		CommonModel o	=	new CommonModel();
		System.out.println(o.parseTime("2011-08-15 00:00:00"));
	}
}

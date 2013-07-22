package etu.splunk.hbase;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

public class TestJson {
	
	private ObjectMapper mapper ;

	@Before
	public void setUp() throws Exception {
		mapper	=	new ObjectMapper();
	}
	@Test
	public void testJson() throws Exception{
		List<Map<String,String>> list	=	new ArrayList<Map<String,String>>();
		list.add(this.parse("409,7218,2011-08-15 23:00:00,GWB,HUB,000,1,0,0,CN,1"));
		list.add(this.parse("409,7649,2011-08-15 23:00:00,CHN,GD,301,1,5,3675,CN,5"));
		list.add(this.parse("409,8992,2011-08-15 23:00:00,CHN,HAN,206,1,0,7738070,CN,51"));
		list.add(this.parse("409,3964,2011-08-15 21:00:00,CHN,JS,206,1,0,6492,CN,2"));
		list.add(this.parse("409,5003,2011-08-15 20:00:00,CHN,JS,302,1,1,592,CN,1"));
		list.add(this.parse("853,6338,2011-08-15 00:00:00,UNI,JS,200,0,0,1906,CN,1"));
		list.add(this.parse("853,5158,2011-08-15 00:00:00,CHN,BJ,200,0,0,5711,CN,3"));
		list.add(this.parse("853,7568,2011-08-15 00:00:00,UNI,SD,200,1,0,75252,CN,26"));
		list.add(this.parse("853,7569,2011-08-15 00:00:00,UNI,YN,200,1,0,2208,CN,1"));
		StringWriter writer  = new StringWriter();
		mapper.writeValue(writer, list);
		System.out.println(writer.toString());
	}
	
	private Map<String,String> parse(String line){
		String []strs	=	StringUtils.split(line,",");
		Map<String,String> map	= new HashMap<String,String>();
		map.put("uid", strs[0]);
		map.put("cid", strs[1]);
		map.put("country", strs[9]);
		map.put("province", strs[4]);
		map.put("isp", strs[3]);
		map.put("httpcode", strs[5]);
		map.put("hit", strs[6]);
		map.put("pv", strs[7]);
		map.put("req", strs[10]);
		map.put("flux", strs[8]);
		
		return map;
	}
}

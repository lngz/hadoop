

String dnsCacheLog[] = value.toString().split("\\|");
        	if(dnsCacheLog.length>1){
	        	//debug System.out.println(dnsCacheLog[1]);
	        	word.set(dnsCacheLog[1]);
	        	context.write(word, one);
        	}
给dns日志用。
日志用|分割
计数|分割的第二项

/hadoop-1.2.1/bin/hadoop jar wc.jar WordCount <in> <out>





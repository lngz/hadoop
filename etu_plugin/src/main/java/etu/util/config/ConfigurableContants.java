package etu.util.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

public class ConfigurableContants {

    private static final String DEFAULT_VALUE = "";

    private static final Logger logger = Logger
            .getLogger(ConfigurableContants.class);

    private static final Properties p = new Properties();

    static {
        init("/system.properties");
    }

    private static void init(String propertyFileName) {

        InputStream inputStream = null;

        try {

            inputStream = ConfigurableContants.class
                    .getResourceAsStream(propertyFileName);
            p.load(inputStream);
            inputStream.close();
        } catch (IOException e) {
            logger.error("load " + propertyFileName + " into Contants error");
        }
    }

    protected static String getProperty(String key, String defaultValue) {
        return p.getProperty(key, defaultValue);
    }

    protected static String getProperty(String key) {
        return getProperty(key, DEFAULT_VALUE);
    }

    /**
     * 取以","分割的集合属性
     */
    protected static Set<String> getSetProperty(String key, String defaultValue) {

        String[] strings = p.getProperty(key, defaultValue).split(",");
        HashSet<String> hashSet = new HashSet<String>(strings.length);
        for (String string : strings) {
            hashSet.add(string.trim());
        }
        return hashSet;
    }

    protected static Set<String> getSetProperty(String key) {
        return getSetProperty(key, DEFAULT_VALUE);
    }

    /**
     * 取以":"分隔，再以","分隔的映射
     */
    protected static Map<String, Integer> getMapProperty(String key) {

        String[] mappings = p.getProperty(key).split(":");
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (String entry : mappings) {
            String[] splits = entry.split(",");
            map.put(splits[0].trim(), Integer.parseInt(splits[1].trim()));
        }
        return map;
    }

}
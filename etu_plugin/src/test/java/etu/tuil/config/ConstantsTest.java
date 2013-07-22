package etu.tuil.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import etu.util.config.Constants;

public class ConstantsTest{
	@Test
    public void testConstants() {
        assertEquals(Constants.SPLIT,",");
        assertEquals(Constants.PARAM_SPLIT,"_");
        assertEquals(Constants.COLUMN_SPLIT,":");
        assertEquals(Constants.FILTER_SPLIT,"-");
        assertEquals(Constants.ZOOKEEPER_CLIENT_PORT,"2181");
        assertTrue(Constants.BATCH_SIZE==1000);
    }

}
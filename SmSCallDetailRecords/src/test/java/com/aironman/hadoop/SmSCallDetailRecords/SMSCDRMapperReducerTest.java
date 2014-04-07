package com.aironman.hadoop.SmSCallDetailRecords;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.aironman.hadoop.SmSCallDetailRecords.SmsCDRMapper.CDRCounter;

public class SMSCDRMapperReducerTest {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	Configuration conf = new Configuration();

	@Before
	public void setUp() {
		
		SmsCDRMapper mapper = new SmsCDRMapper();
		SmsCDRReducer reducer = new SmsCDRReducer();
		
		mapDriver = MapDriver.newMapDriver(mapper);
		//Passing arguments for Testing, but its deprecated
		conf.set("myParameter1", "20");
		conf.set("myParameter2", "23");
		mapDriver.setConfiguration(conf);
		
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws Exception {
		mapDriver.withInput(new LongWritable(), new Text(
				"655209;1;796764372490213;804422938115889;6"));
		mapDriver.withOutput(new Text("6"), new IntWritable(1));
		mapDriver.runTest();

	}

	@Test
	public void testMapperWillFail() throws Exception {
		mapDriver.withInput(new LongWritable(), new Text(
				"655209;0;796764372490213;804422938115889;6"));
		// mapDriver.withOutput(new Text("6"), new IntWritable(1));
		mapDriver.runTest();
		assertEquals("Expected 1 counter increment", 0,
				mapDriver.getCounters().findCounter(CDRCounter.NonSMSCDR).getValue());
	}

	@Test
	public void testReducer() throws Exception {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("6"), values);
		reduceDriver.withOutput(new Text("6"), new IntWritable(2));
		reduceDriver.runTest();
	}
}
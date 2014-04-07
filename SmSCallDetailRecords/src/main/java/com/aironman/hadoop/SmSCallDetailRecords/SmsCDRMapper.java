package com.aironman.hadoop.SmSCallDetailRecords;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/***
 * The records look like: 
 * CDRID ; CDRType ; Phone1 		; Phone2 		  	; SMS Status Code
 * 655209; 1 	   ; 796764372490213; 804422938115889 	; 6 
 * 353415; 0	   ; 356857119806206 ; 287572231184798  ; 4 
 * 835699; 1	   ; 252280313968413 ; 889717902341635 	; 0
 * 
 * The MapReduce program analyzes these records, finds all records with CDRType
 * as 1, and note its corresponding SMS Status Code. For example, the Mapper
 * outputs are 6, 1 0, 1
 * 
 * @author aironman
 *
 */
public class SmsCDRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text status = new Text();
	private final static IntWritable addOne = new IntWritable(1);

	static enum CDRCounter {
		NonSMSCDR;
	};

	/**
	 * Returns the SMS status code and its count
	 */
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException,
			InterruptedException {

		// 655209;1;796764372490213;804422938115889;6 is the Sample record
		// format
		System.out.println(" =value= " + value.toString());
		String[] line = value.toString().split(";");
		for (int i=0;i<line.length;i++)
			System.out.println(" =line= " + i + ": " + line[i]);
		// If record is of SMS CDR
		if (Integer.parseInt(line[1]) == 1) {
			status.set(line[4]);
			context.write(status, addOne);
		}else {// CDR record is not of type SMS so increment the counter // this will stop mapping tasks!
		      context.getCounter(CDRCounter.NonSMSCDR).increment(1);
			System.out.println("CDR record " + line[1] +" is not of type SMS.");
		}
	}
}
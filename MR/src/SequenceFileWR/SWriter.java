package SequenceFileWR;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;


/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public class SWriter {
    
	public static final String path="/hadoop";
	
	public static void main(String args[]) {
		JobConf conf = new JobConf(SWriter.class);
		Path sortWF=new Path(path,"sortFile1");
	    FileSystem fs=null;
		try {
			fs = sortWF.getFileSystem(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		SequenceFile.Writer out=null;
	    try {
			out=SequenceFile.createWriter(fs,conf,sortWF,LongWritable.class,Text.class,CompressionType.NONE,null,null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String refer="Without question, many of us have mastered the neurotic art of spending" +
				" much of our lives worrying about variety of things -- all at once. We allow" +
				" past problems and future concerns to dominate your present moments, so much" +
				" so that we end up anxious, frustrated, depressed, and hopeless. On the flip" +
				" side, we also postpone our gratification, our stated priorities, and our happiness," +
				" often convincing ourselves that someday will be much better than today. Unfortunately," +
				" the same mental dynamics that tell us to look toward the future will only repeat" +
				" themselves so that 'someday' never actually arrives. John Lennone once said," +
			    " Life is what is happening while we are busy making other plans.When we are busy " +
				" making 'other plans', our children are busy growing up, the people we love are moving" +
				" away and dying, our bodies are getting out of shape, and our dreams are slipping away." +
				" In short, we miss out on life.";
		String referArrays[]=refer.split("[\\s]+?");
		int arrLength=referArrays.length;
		Random r=new Random();
		for(int i=0;i<10000;i++) {
	     LongWritable key=new LongWritable(r.nextLong());
		 Text value=new Text(referArrays[r.nextInt(arrLength)]);
		 try {
			out.append(key,value);
		 } catch (IOException e) {
			e.printStackTrace();
		 }
		}
		try {
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

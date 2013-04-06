package SequenceFileWR;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

public class SReader {
    
	public static final String path="/hadoop";
	
	public static void main(String args[]) {
		JobConf conf = new JobConf(SReader.class);	
		Path sortRF=new Path(path,"sortFile1");
		FileSystem fs=null;
		try {
			fs = sortRF.getFileSystem(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		SequenceFile.Reader in=null;
		try {
			in=new SequenceFile.Reader(fs,sortRF,conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Text val=ReflectionUtils.newInstance(Text.class,conf);
		LongWritable key=ReflectionUtils.newInstance(LongWritable.class,conf);
		System.out.println("Test");
		boolean remaining=false;
		try {
			remaining = in.next(key);
		} catch (IOException e) {
			e.printStackTrace();
		}
		int i=0;
		while(remaining&&i<50) {
			try {
				in.getCurrentValue(val);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			System.out.println((i+1)+" key:"+key+" value:"+val.toString());
			i++;
			try {
				remaining = in.next(key);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

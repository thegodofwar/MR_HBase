package statistics;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class Statistics {
	
	static Configuration cfg=null; 
	
    static {   
    	cfg=HBaseConfiguration.create();
    }
	
    public static final Log LOG=LogFactory.getLog(Statistics.class);
    
	public static final String sourceTable = "hbNew";
	
	public static final String targetTable = "hbNewStatistics"; 
	
	public static void createTable(String tablename,String family) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tablename)) {
            System.out.println("table   Exists!!!");
        }
        else{
            HTableDescriptor tableDesc = new HTableDescriptor(tablename);
            tableDesc.addFamily(new HColumnDescriptor(family));
            admin.createTable(tableDesc);
            System.out.println("create table ok .");
        }
    }
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		try {
			createTable(targetTable,"sta");
		} catch (Exception e) {
			e.printStackTrace();
		}
		Job job = new Job(cfg,"statistics");
		job.setJarByClass(Statistics.class);
		Scan scan = new Scan();
		scan.setCaching(1024);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob(sourceTable, scan, MyMapper.class,
				Text.class, IntWritable.class, job);
		TableMapReduceUtil
				.initTableReducerJob(targetTable, MyReducer.class, job);
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error");
		}
	}
	
	public static class MyMapper extends TableMapper<Text, IntWritable> {
		private final IntWritable ONE = new IntWritable(1);
		private Text text = new Text();
		private int count=0;

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			text.set(new String(row.get()));
			if(count<5) {
			  for (KeyValue kv : value.list()) {
				LOG.info("map: found row:" +
			    Bytes.toString(kv.getRow()) + ", column:" +
			    Bytes.toString(kv.getQualifier())+" value:"+Bytes.toString(kv.getValue()));
				context.write(text,ONE);
			  }
			  count++;
			}
		}
	}
	
	public static class MyReducer extends
			TableReducer<Text, IntWritable, ImmutableBytesWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int i = 0;
			for (IntWritable val : values) {
				i += val.get();
			}
			LOG.info("reduce:"+key);
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("sta"),null, Bytes.toBytes(i));
			context.write(new ImmutableBytesWritable(key.getBytes()),put);
		}
	}
	
}

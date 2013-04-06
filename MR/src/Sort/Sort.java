package Sort;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.myorg.WordCount;
import org.myorg.WordCount.Map;
import org.myorg.WordCount.Reduce;

public class Sort<K,V> extends Configured implements Tool {
  private static final String path="/hadoop/";
  
  private RunningJob jobResult = null;

  static int printUsage() {
    System.out.println("sort [-m <maps>] [-r <reduces>] " +
                       "[-inFormat <input format class>] " +
                       "[-outFormat <output format class>] " + 
                       "[-outKey <output key class>] " +
                       "[-outValue <output value class>] " +
                       "[-totalOrder <pcnt> <num samples> <max splits>] " +
                       "<input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * The main driver for sort program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public int run(String[] args) throws Exception {

    JobConf jobConf = new JobConf(getConf(), Sort.class);
    jobConf.setJobName("sorter");

    jobConf.setMapperClass(IdentityMapper.class);        
    jobConf.setReducerClass(IdentityReducer.class);

    JobClient client = new JobClient(jobConf);
    ClusterStatus cluster = client.getClusterStatus();
    System.out.println("ClusterStatus:"+Arrays.toString(cluster.getActiveTrackerNames().toArray(new String[]{})));
    int num_reduces = (int) (cluster.getMaxReduceTasks() * 0.9);
    String sort_reduces = jobConf.get("test.sort.reduces_per_host");
    System.out.println("test.sort.reduces_per_host:"+sort_reduces);
    if (sort_reduces != null) {
       num_reduces = cluster.getTaskTrackers() * 
                       Integer.parseInt(sort_reduces);
    }
    
    Class<? extends InputFormat> inputFormatClass = 
      SequenceFileInputFormat.class;
    Class<? extends OutputFormat> outputFormatClass = 
      SequenceFileOutputFormat.class;
    Class<? extends WritableComparable> outputKeyClass = BytesWritable.class;
    Class<? extends Writable> outputValueClass = BytesWritable.class;
    List<String> otherArgs = new ArrayList<String>();
    InputSampler.Sampler<K,V> sampler = null;
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          jobConf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          num_reduces = Integer.parseInt(args[++i]);
        } else if ("-inFormat".equals(args[i])) {
          inputFormatClass = 
            Class.forName(args[++i]).asSubclass(InputFormat.class);
        } else if ("-outFormat".equals(args[i])) {
          outputFormatClass = 
            Class.forName(args[++i]).asSubclass(OutputFormat.class);
        } else if ("-outKey".equals(args[i])) {
          outputKeyClass = 
            Class.forName(args[++i]).asSubclass(WritableComparable.class);
        } else if ("-outValue".equals(args[i])) {
          outputValueClass = 
            Class.forName(args[++i]).asSubclass(Writable.class);
        } else if ("-totalOrder".equals(args[i])) {
          double pcnt = Double.parseDouble(args[++i]);
          int numSamples = Integer.parseInt(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler =
            new InputSampler.RandomSampler<K,V>(pcnt, numSamples, maxSplits);
        } else {
          otherArgs.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
            args[i-1]);
        return printUsage(); // exits
      }
    }

    // Set user-supplied (possibly default) job configs
    jobConf.setNumReduceTasks(num_reduces);

    jobConf.setInputFormat(inputFormatClass);
    jobConf.setOutputFormat(outputFormatClass);

    jobConf.setOutputKeyClass(outputKeyClass);
    jobConf.setOutputValueClass(outputValueClass);

    // Make sure there are exactly 2 parameters left.
    if (otherArgs.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
          otherArgs.size() + " instead of 2.");
      return printUsage();
    }
    FileInputFormat.setInputPaths(jobConf, otherArgs.get(0));
    FileOutputFormat.setOutputPath(jobConf, new Path(otherArgs.get(1)));

    if (sampler != null) {
      System.out.println("Sampling input to effect total-order sort...");
      jobConf.setPartitionerClass(TotalOrderPartitioner.class);
      Path inputDir = FileInputFormat.getInputPaths(jobConf)[0];
      inputDir = inputDir.makeQualified(inputDir.getFileSystem(jobConf));
      Path partitionFile = new Path(inputDir, "_sortPartitioning");
      TotalOrderPartitioner.setPartitionFile(jobConf, partitionFile);
      InputSampler.<K,V>writePartitionFile(jobConf, sampler);
      URI partitionUri = new URI(partitionFile.toString() +
                                 "#" + "_sortPartitioning");
      DistributedCache.addCacheFile(partitionUri, jobConf);
      DistributedCache.createSymlink(jobConf);
    }
    
    setConf(jobConf);
    System.out.println("Running on " +
        cluster.getTaskTrackers() +
        " nodes to sort from " + 
        FileInputFormat.getInputPaths(jobConf)[0] + " into " +
        FileOutputFormat.getOutputPath(jobConf) +
        " with " + num_reduces + " reduces.");
    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    jobResult = JobClient.runJob(jobConf);
    Date end_time = new Date();
    System.out.println("Job ended: " + end_time);
    System.out.println("The job took " + 
        (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
    return 0;
  }



 /* public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Sort(), args);
    System.exit(res);
  }*/
  
  public static void main(String[] args) throws Exception {
	    JobConf jobConf = new JobConf(Sort.class);
	    jobConf.setJobName("sorter");

	    jobConf.setMapperClass(IdentityMapper.class);        
	    jobConf.setReducerClass(IdentityReducer.class);

	    JobClient client = new JobClient(jobConf);
	    ClusterStatus cluster = client.getClusterStatus();
	    System.out.println("ClusterStatus:"+Arrays.toString(cluster.getActiveTrackerNames().toArray(new String[]{})));
	    int num_reduces = (int) (cluster.getMaxReduceTasks() * 0.9);
	    System.out.println("num_reduces="+num_reduces);
	    String sort_reduces = jobConf.get("test.sort.reduces_per_host");
	    System.out.println("test.sort.reduces_per_host:"+sort_reduces);
	    if (sort_reduces != null) {
	       num_reduces = cluster.getTaskTrackers() * 
	                       Integer.parseInt(sort_reduces);
	    }
	    
	    Class<? extends InputFormat> inputFormatClass = 
	      SequenceFileInputFormat.class;
	    Class<? extends OutputFormat> outputFormatClass = 
	      SequenceFileOutputFormat.class;
	    Class<? extends WritableComparable> outputKeyClass = BytesWritable.class;
	    Class<? extends Writable> outputValueClass = BytesWritable.class;
	    List<String> otherArgs = new ArrayList<String>();
	    InputSampler.Sampler sampler = null;
	    for(int i=0; i < args.length; ++i) {
	      try {
	        if ("-m".equals(args[i])) {
	          jobConf.setNumMapTasks(Integer.parseInt(args[++i]));
	        } else if ("-r".equals(args[i])) {
	          num_reduces = Integer.parseInt(args[++i]);
	        } else if ("-inFormat".equals(args[i])) {
	          inputFormatClass = 
	            Class.forName(args[++i]).asSubclass(InputFormat.class);
	        } else if ("-outFormat".equals(args[i])) {
	          outputFormatClass = 
	            Class.forName(args[++i]).asSubclass(OutputFormat.class);
	        } else if ("-outKey".equals(args[i])) {
	          outputKeyClass = 
	            Class.forName(args[++i]).asSubclass(WritableComparable.class);
	        } else if ("-outValue".equals(args[i])) {
	          outputValueClass = 
	            Class.forName(args[++i]).asSubclass(Writable.class);
	        } else if ("-totalOrder".equals(args[i])) {
	          double pcnt = Double.parseDouble(args[++i]);
	          int numSamples = Integer.parseInt(args[++i]);
	          int maxSplits = Integer.parseInt(args[++i]);
	          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
	          sampler =
	            new InputSampler.RandomSampler(pcnt, numSamples, maxSplits);
	        } else {
	          otherArgs.add(args[i]);
	        }
	      } catch (NumberFormatException except) {
	        System.out.println("ERROR: Integer expected instead of " + args[i]);
	      } catch (ArrayIndexOutOfBoundsException except) {
	        System.out.println("ERROR: Required parameter missing from " +
	            args[i-1]);
	      }
	    }

	    // Set user-supplied (possibly default) job configs
	    jobConf.setNumReduceTasks(num_reduces);

	    jobConf.setInputFormat(inputFormatClass);
	    jobConf.setOutputFormat(outputFormatClass);

	    jobConf.setOutputKeyClass(outputKeyClass);
	    jobConf.setOutputValueClass(outputValueClass);

	    // Make sure there are exactly 2 parameters left.
	    if (otherArgs.size() != 2) {
	      System.out.println("ERROR: Wrong number of parameters: " +
	          otherArgs.size() + " instead of 2.");
	    }
	    FileInputFormat.setInputPaths(jobConf, otherArgs.get(0));
	    FileOutputFormat.setOutputPath(jobConf, new Path(otherArgs.get(1)));

	    if (sampler != null) {
	      System.out.println("Sampling input to effect total-order sort...");
	      jobConf.setPartitionerClass(TotalOrderPartitioner.class);
	      Path inputDir = FileInputFormat.getInputPaths(jobConf)[0];
	      inputDir = inputDir.makeQualified(inputDir.getFileSystem(jobConf));
	      Path partitionFile = new Path(inputDir, "_sortPartitioning");
	      TotalOrderPartitioner.setPartitionFile(jobConf, partitionFile);
	      InputSampler.writePartitionFile(jobConf, sampler);
	      URI partitionUri = new URI(partitionFile.toString() +
	                                 "#" + "_sortPartitioning");
	      DistributedCache.addCacheFile(partitionUri, jobConf);
	      DistributedCache.createSymlink(jobConf);
	    }
	    
	    System.out.println("Running on " +
	        cluster.getTaskTrackers() +
	        " nodes to sort from " + 
	        FileInputFormat.getInputPaths(jobConf)[0] + " into " +
	        FileOutputFormat.getOutputPath(jobConf) +
	        " with " + num_reduces + " reduces.");
	    Date startTime = new Date();
	    System.out.println("Job started: " + startTime);
	    RunningJob jobResult = JobClient.runJob(jobConf);
	    Date end_time = new Date();
	    System.out.println("Job ended: " + end_time);
	    System.out.println("The job took " + 
	        (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
	}

  /**
   * Get the last job that was run using this instance.
   * @return the results of the last job that was run
   */
  public RunningJob getResult() {
    return jobResult;
  }
}

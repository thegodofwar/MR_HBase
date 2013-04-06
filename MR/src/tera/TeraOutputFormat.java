package tera;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * A streamlined text output format that writes key, value, and "\r\n".
 */
public class TeraOutputFormat extends TextOutputFormat<Text,Text> {
  static final String FINAL_SYNC_ATTRIBUTE = "terasort.final.sync";

  /**
   * Set the requirement for a final sync before the stream is closed.
   */
  public static void setFinalSync(JobConf conf, boolean newValue) {
    conf.setBoolean(FINAL_SYNC_ATTRIBUTE, newValue);
  }

  /**
   * Does the user want a final sync at close?
   */
  public static boolean getFinalSync(JobConf conf) {
    return conf.getBoolean(FINAL_SYNC_ATTRIBUTE, false);
  }

  static class TeraRecordWriter extends LineRecordWriter<Text,Text> {
    private static final byte[] newLine = "\r\n".getBytes();
    private boolean finalSync = false;

    public TeraRecordWriter(DataOutputStream out,
                            JobConf conf) {
      super(out);
      finalSync = getFinalSync(conf);
    }

    public synchronized void write(Text key, 
                                   Text value) throws IOException {
      out.write(key.getBytes(), 0, key.getLength());
      out.write(value.getBytes(), 0, value.getLength());
      out.write(newLine, 0, newLine.length);
    }
    
    public void close() throws IOException {
      if (finalSync) {
        ((FSDataOutputStream) out).sync();
      }
      super.close(null);
    }
  }

  public RecordWriter<Text,Text> getRecordWriter(FileSystem ignored,
                                                 JobConf job,
                                                 String name,
                                                 Progressable progress
                                                 ) throws IOException {
    Path dir = getWorkOutputPath(job);
    FileSystem fs = dir.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
    return new TeraRecordWriter(fileOut, job);
  }
}


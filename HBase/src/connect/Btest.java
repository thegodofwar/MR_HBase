package connect;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class Btest{
	static Configuration cfg=null; 
    static {   
    	cfg=HBaseConfiguration.create();
    }
    /**
     * 创建一张表
     */
    public static void creatTable(String tablename,String family) throws Exception {
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
    
    public static void creatTableNew(String tablename) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tablename)) {
            System.out.println("table   Exists!!!");
        } else{
        	HTableDescriptor tableDescriptor = new HTableDescriptor(tablename);  
            tableDescriptor.addFamily(new HColumnDescriptor("umn1"));  
            tableDescriptor.addFamily(new HColumnDescriptor("umn2"));  
            tableDescriptor.addFamily(new HColumnDescriptor("umn3"));
            admin.createTable(tableDescriptor); 
            System.out.println("create table ok .");
        }
    }
    
    public static void addDataNew(String tablename) {
    	System.out.println("start insert data ......");  
        HTablePool pool = new HTablePool(cfg, 1000);  
        HTableInterface table=pool.getTable(tablename); 
        table.setAutoFlush(false);
        try {
			table.setWriteBufferSize(6*1024*1024);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
        List<Put> puts=new ArrayList<Put>();
        for(int i=1;i<=6;i++) {
          Put put = new Put((i+"abc").getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值  
          put.add("umn1".getBytes(), "a".getBytes(), "aaa".getBytes());// 本行数据的第一列  
          put.add("umn2".getBytes(), "b".getBytes(), "bbb".getBytes());// 本行数据的第三列  
          put.add("umn3".getBytes(), "c".getBytes(), "ccc".getBytes());// 本行数据的第三列  
          puts.add(put);
        }
        try {  
            table.put(puts);
            table.flushCommits();
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
        System.out.println("end insert data ......"); 
    }
   
    public static void Query(String tableName) {  
        HTablePool pool = new HTablePool(cfg, 1000);  
        HTableInterface table=pool.getTable(tableName);
        List<Get> batch= new ArrayList<Get>();  
        Get get1=new Get("1abc".getBytes());  
        Get get2=new Get("3abc".getBytes());  
        Get get3=new Get("5abc".getBytes());
          
        batch.add(get1);  
        batch.add(get2);  
        batch.add(get3);
        
        Result[] results=null;
		try {
			results = table.get(batch);
		} catch (IOException e) {
			e.printStackTrace();
		} 
		try {  
            for (Result r : results) { 
            	if(!r.isEmpty()) {
            		System.out.println("获得到rowkey:" + new String(r.getRow()));  
                    for (KeyValue keyValue : r.raw()) {  
                        System.out.println("row:"+new String(keyValue.getRow())+" 列：" + new String(keyValue.getFamily())  
                                + "====key:"+new String(keyValue.getQualifier())+"   value:"+ new String(keyValue.getValue()));  
                    } 
            	}
            }  
            table.close();
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        System.out.println("result size : "+results.length);
    }
    
    public static void QueryAll(String tableName) {  
        HTablePool pool = new HTablePool(cfg, 1000);  
        HTableInterface table=pool.getTable(tableName);
        try {  
            ResultScanner rs = table.getScanner(new Scan());  
            for (Result r : rs) {  
                System.out.println("获得到rowkey:" + new String(r.getRow())); 
                for (Map.Entry<byte[], byte[]> entry : r.getFamilyMap("umn1".getBytes()).entrySet()) {  
                    System.out.println("key:" + new String(entry.getKey())+"   value:"+new String(entry.getValue()));  
                }  
            }  
            rs.close();
            table.close();
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
    
    /** 
     * 查询所有数据 
     * @param tableName 
     */  
    public static void QueryAll1(String tableName) {  
        HTablePool pool = new HTablePool(cfg,1000);  
        HTableInterface table = pool.getTable(tableName);  
        try {  
            ResultScanner rs = table.getScanner(new Scan());  
            for (Result r : rs) {  
                System.out.println("获得到rowkey:" + new String(r.getRow()));  
                for (KeyValue keyValue : r.raw()) {  
                    System.out.println("row:"+new String(keyValue.getRow())+" 列：" + new String(keyValue.getFamily())  
                            + "====key:"+new String(keyValue.getQualifier())+"   value:"+ Bytes.toInt(keyValue.getValue()));  
                }  
            }  
            rs.close();
            table.close();
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
    
    public static void QueryByCondition1(String tableName) {  
        HTablePool pool = new HTablePool(cfg,1000);  
        HTableInterface table = pool.getTable(tableName);  
        try {  
            Get scan = new Get("3abc".getBytes());// 根据rowkey查询  
            Result r = table.get(scan);  
            System.out.println("获得到rowkey:" + new String(r.getRow()));  
            for (KeyValue keyValue : r.raw()) {  
                System.out.println("列：" + new String(keyValue.getFamily())  
                        + "====key:"+new String(keyValue.getQualifier())+"   value:"+ new String(keyValue.getValue())); 
            }  
            table.close();
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
    
    public static void QueryByCondition2(String tableName) {  
        try {  
            HTablePool pool = new HTablePool(cfg, 1000);  
            HTableInterface table = pool.getTable(tableName);  
            Filter filter = new SingleColumnValueFilter(Bytes  
                    .toBytes("umn2"),Bytes.toBytes("c"), CompareOp.EQUAL, Bytes  
                    .toBytes("aaa")); // 当列column1的值为aaa时进行查询  
            Scan s = new Scan();  
            s.setFilter(filter);  
            ResultScanner rs = table.getScanner(s);  
            for (Result r : rs) {  
                System.out.println("获得到rowkey:" + new String(r.getRow()));  
                for (KeyValue keyValue : r.raw()) {  
                    System.out.println("列：" + new String(keyValue.getFamily())  
                            + "====key:"+new String(keyValue.getQualifier())+"   value:"+ new String(keyValue.getValue()));  
                }  
            }  
            rs.close();
            table.close();
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    }  
    
    public static void QueryByCondition3(String tableName) {
    	System.out.println("********QueryByCondition3*******");
        try {  
            HTablePool pool = new HTablePool(cfg, 1000);  
            HTableInterface table = pool.getTable(tableName);  
  
            List<Filter> filters = new ArrayList<Filter>();  
  
            Filter filter1 = new SingleColumnValueFilter(Bytes  
                    .toBytes("umn"), null, CompareOp.EQUAL, Bytes  
                    .toBytes("ggg"));  
            filters.add(filter1);  
  
            Filter filter2 = new SingleColumnValueFilter(Bytes  
                    .toBytes("umn"), null, CompareOp.EQUAL, Bytes  
                    .toBytes("fff"));  
            filters.add(filter2);  
  
            Filter filter3 = new SingleColumnValueFilter(Bytes  
                    .toBytes("umn"), null, CompareOp.EQUAL, Bytes  
                    .toBytes("ddd"));  
            filters.add(filter3);  
  
            FilterList filterList1 = new FilterList(filters);  
  
            Scan scan = new Scan();  
            scan.setFilter(filterList1);  
            ResultScanner rs = table.getScanner(scan);  
            for (Result r : rs) {  
                System.out.println("获得到rowkey:" + new String(r.getRow()));  
                for (KeyValue keyValue : r.raw()) {  
                    System.out.println("列：" + new String(keyValue.getFamily())  
                            + "====key:"+new String(keyValue.getQualifier())+"   value:"+ new String(keyValue.getValue()));  
                }  
            }  
            rs.close(); 
            table.close();
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
  
    }
    
    public static void deleteRow(String tablename, String rowkey)  {  
        try {  
            HTable table = new HTable(cfg,tablename);  
            List<Delete> list = new ArrayList<Delete>();  
            Delete d1 = new Delete(rowkey.getBytes());  
            list.add(d1);  
            table.delete(list);  
            System.out.println("删除行成功!");  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    } 
    
    /**
     * 添加一条数据
     */
    public static void addData (String tablename) throws Exception{
         HTable table = new HTable(cfg, tablename);
         List<Put> puts=new ArrayList<Put>();
         for (int i = 0; i < 3; i++) {   //for is number of rows
             Put putRow = new Put(("row" + i).getBytes()); //the ith row
             putRow.add("nf".getBytes(), "col1".getBytes(), "vaule1"
                     .getBytes());  //set the name of column and value.
             putRow.add("nf".getBytes(), "col2".getBytes(), "vaule2"
                     .getBytes());
             putRow.add("nf".getBytes(), "col3".getBytes(), "vaule3"
                     .getBytes());
             puts.add(putRow);
         }
         table.put(puts);
         System.out.println("add data ok .");
    }
   
    /**
     * 显示所有数据
     */
    public static void getAllData (String tablename) throws Exception{
         HTable table = new HTable(cfg, tablename);
         Scan s = new Scan();
         ResultScanner ss = table.getScanner(s);
         for(Result r:ss){
             for(KeyValue kv:r.raw()){
                System.out.print(new String(kv.getKey(),"utf-8")+"******");
                System.out.println(new String(kv.getValue(),"utf-8"));
             }
             System.out.println();
         }
         ss.close();
    }
    
    public static void getAllDataNew(String tablename) {
    	HTable table=null;
    	try {
			table = new HTable(cfg,tablename);
		} catch (IOException e) {
			e.printStackTrace();
		} 
		ResultScanner ss=null;
		try {
			ss = table.getScanner("nf".getBytes("utf-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(Result result:ss){//get data of column clusters 
	        try {
				for(Map.Entry<byte[], byte[]> entry : result.getFamilyMap("nf".getBytes("utf-8")).entrySet()){//get collection of result
				    String column=new String(entry.getKey(),"utf-8");
				    String value=new String(entry.getValue(),"utf-8");
				    System.out.println(column+","+value);
				}
				System.out.println();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
	    }
    }
    
    public static void dropTable(String tableName) {  
        try {  
            HBaseAdmin admin = new HBaseAdmin(cfg);  
            admin.disableTable(tableName);  
            admin.deleteTable(tableName);  
        } catch (MasterNotRunningException e) {  
            e.printStackTrace();  
        } catch (ZooKeeperConnectionException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
    
    public static void insertDatas() {
    	String family = "Family";
        
    	String table = "TestColumnRangeFilterClient";
    	
    	try {
			creatTable(table,family);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
    	
    	HTable ht=null;
		try {
			ht = new HTable(cfg,table);
		} catch (IOException e1) {
			e1.printStackTrace();
		} 
		ht.setAutoFlush(false);
		try {
			ht.setWriteBufferSize(6*1024*1024);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
    	Map<StringRange, List<String>> rangeMap = new HashMap<StringRange, List<String>>();

        rangeMap.put(new StringRange(null, true, "b", false),
            new ArrayList<String>());
        rangeMap.put(new StringRange("p", true, "q", false),
            new ArrayList<String>());
        rangeMap.put(new StringRange("r", false, "s", true),
            new ArrayList<String>());
        rangeMap.put(new StringRange("z", false, null, false),
            new ArrayList<String>());
        String valueString = "ValueString";
    	
    	BufferedWriter out=null;
	    try {
			out=new BufferedWriter(new FileWriter(new File("datas.txt"),true));
	    } catch (IOException e) {
		    e.printStackTrace();
	    }
    	List<String> rows = generateRandomWords(10, 8);
        for(String r:rows) {
        	try {
        		out.write(r);
        		out.write("\r");
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        try {
			out.write("*********************************************************************************************************************************");
			out.write("\r\n");
        } catch (IOException e) {
			e.printStackTrace();
		}
        
        List<String> columns = generateRandomWords(2000, 8);
        for(String c : columns) {
        	try {
        		out.write(c);
        		out.write("\r");
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        try {
			 out.flush();
			 out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		int maxTimestamp=2;
		List<Put> puts=new ArrayList<Put>();
	    for (String row : rows) {
	        Put p = new Put(Bytes.toBytes(row));
	        p.setWriteToWAL(false);
	        for (String column : columns) {
	          for (long timestamp = 1; timestamp <= maxTimestamp; timestamp++) {
	            KeyValue kv = KeyValueTestUtil.create(row, family, column, timestamp, valueString);
	            try {
					p.add(kv);
				} catch (IOException e) {
					e.printStackTrace();
				}
	            for (StringRange s : rangeMap.keySet()) {
	              if (s.inRange(column)) {
	                rangeMap.get(s).add(column);
	              }
	            }
	          }
	        }
	        puts.add(p);
	     }
	    try {
	    	ht.put(puts);
			ht.flushCommits();
			ht.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		BufferedWriter statisticsOut=null;
	    try {
	    	statisticsOut=new BufferedWriter(new FileWriter(new File("staDatas.txt"),true));
	    } catch (IOException e) {
		    e.printStackTrace();
	    }
		for (StringRange s : rangeMap.keySet()) {
              List<String> list=rangeMap.get(s);
              for(String str:list) {
            	  try {
            		statisticsOut.write(s.toString());
            		statisticsOut.write("\t");
					statisticsOut.write(str);
					statisticsOut.write("\r");
				  } catch (IOException e) {
					e.printStackTrace();
				 }
              }
        }
		try {
			statisticsOut.flush();
			statisticsOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
    }
    
    public static List<String> generateRandomWords(int numberOfWords, int maxLengthOfWords) {
        Set<String> wordSet = new HashSet<String>();
        for (int i = 0; i < numberOfWords; i++) {
          int lengthOfWords = (int) (Math.random() * maxLengthOfWords) + 1;
          char[] wordChar = new char[lengthOfWords];
          for (int j = 0; j < wordChar.length; j++) {
            wordChar[j] = (char) (Math.random() * 26 + 97);
          }
          String word = new String(wordChar);
          wordSet.add(word);
        }
        List<String> wordList = new ArrayList<String>(wordSet);
        return wordList;
    }
    
    public static void columnRangeFilter(StringRange s) {
    	String table = "TestColumnRangeFilterClient";
    	HTable ht=null;
		try {
			ht = new HTable(cfg,table);
		} catch (IOException e1) {
			e1.printStackTrace();
		} 
		ColumnRangeFilter filter;
		Scan scan = new Scan();
		scan.setMaxVersions();
		filter = new ColumnRangeFilter(s.getStart() == null ? null : Bytes
				.toBytes(s.getStart()), s.isStartInclusive(),
				s.getEnd() == null ? null : Bytes.toBytes(s.getEnd()), s
						.isEndInclusive());
		scan.setFilter(filter);
		ResultScanner scanner=null;
		try {
			scanner = ht.getScanner(scan);
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		List<KeyValue> results = new ArrayList<KeyValue>();
		System.out.println("scan column range: " + s.toString());
		long timeBeforeScan = System.currentTimeMillis();

		Result result;
		try {
			while ((result = scanner.next()) != null) {
				for (KeyValue kv : result.list()) {
					results.add(kv);
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		long scanTime = System.currentTimeMillis() - timeBeforeScan;
		scanner.close();
		System.out.println("scan time = " + scanTime + "ms");
		System.out.println("found " + results.size() + " results");

		 int ccc=0;
		 for (KeyValue kv : results) { 
          ccc++;
		  System.out.println(ccc+" found row:" +
		  Bytes.toString(kv.getRow()) + ", column:" +
		  Bytes.toString(kv.getQualifier())+" value:"+Bytes.toString(kv.getValue()));
		 }
		 
		try {
			ht.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    
    public static void  main (String [] agrs) {
        try {
        	 //************创建表、插入、查询数据*********************
             String tablename="hbNewStatistics";
             //Btest.creatTable(tablename);
             //Btest.addData(tablename);
             //Btest.getAllData(tablename);
             //System.out.println("------------------------------------------------------------");
             Btest.QueryAll1(tablename);
             
             //************删除表****************************
        	 //dropTable("TestColumnRangeFilterClient");
             
             //************新创建表、插入、查询数据*****************
             //String table="hbNew";
             //Btest.creatTableNew(table);
             //Btest.addDataNew(table);
             //deleteRow(table,"6abc");
             //Btest.Query(table);
        	 //insertDatas();
        	 //columnRangeFilter(new StringRange("r", false, "s", true));
        	 //columnRangeFilter(new StringRange("p", true, "q", false));
        	 //columnRangeFilter(new StringRange("z", false, null, false));
        	 //columnRangeFilter(new StringRange(null, true, "b", false));
            }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

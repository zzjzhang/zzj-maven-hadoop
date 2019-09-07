package task.hbase;

import org.junit.Test;
import java.util.List;
import org.junit.After;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import java.io.UnsupportedEncodingException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;



/**
 * HADOOP - 2.7.7
 * HBASE - 2.0.1
 * 
 * @author zzj26
 */
public class Task1 implements Runnable {

	// 1. 字段
	private Configuration configuration;
	private Connection connection;
	private Admin admin;
	private TableName tableName;


	// 2. 构造方法
	public Task1() {
		// 
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "192.168.157.129");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		// configuration.set("hbase.master", "10.88.27.136:9001");

		//
		try {
			connection = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//
		tableName = TableName.valueOf("TABLE_1");
	}



	// 3. 业务方法

	// 3-1
	// 创建表
	private void createTable() {
		try {
			admin = connection.getAdmin();

			if(admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			};

//			if(!admin.isTableAvailable(tableName)) {
				// 表 描述器 构造器
	            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);

	            // 列族 描述器 构造器
	            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("user"));

	            // 获得 列 描述器
	            ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();

	            // 添加 列族
	            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);

	            // 获得 表 描述器
	            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

	            // 创建表
	            // admin.addColumnFamily(tableName, columnFamilyDescriptor);     // 给 表 添加 列族
	            
	            admin.createTable(tableDescriptor);
//			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


	/**
	 * 单条插入
	 * 
	 */
	@Test
    public void insertOneData() throws IOException {
		// new 一个列  ，A0001_20181115_01 为 row key
        Put put = new Put(Bytes.toBytes("A0001_20181115_01"));

        // 下面三个分别为：列族、列名、列值
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("Mike"));
        TableName tableName = TableName.valueOf("TABLE_1");

        // 得到 table
        Table table = connection.getTable(tableName);

        // 执行插入
        table.put(put);
    }


    /**
     * 插入多个列
     * 
     */
    @Test
    public void insertManyData() throws IOException {
        Table table = connection.getTable(TableName.valueOf("TABLE_1"));

        List<Put> puts = new ArrayList<Put>();
        Put put1 = new Put(Bytes.toBytes("A0012018111502"));
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("科比"));

        Put put2 = new Put(Bytes.toBytes("A0012018111502"));
        put2.addColumn(Bytes.toBytes("user"), Bytes.toBytes("age"), Bytes.toBytes("38"));

        Put put3 = new Put(Bytes.toBytes("A0012018111502"));
        put3.addColumn(Bytes.toBytes("user"), Bytes.toBytes("weight"), Bytes.toBytes("85kg"));

        Put put4 = new Put(Bytes.toBytes("A0012018111502"));
        put4.addColumn(Bytes.toBytes("user"), Bytes.toBytes("sex"), Bytes.toBytes("男"));

        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);

        table.put(puts);
        table.close();

        connection.close();
    }


    /**
     * 同一条数据的插入
     * 
     */
    @Test
    public void singleRowInsert() throws IOException {
        Table table = connection.getTable(TableName.valueOf("TABLE_1"));

        Put put1 = new Put(Bytes.toBytes("A0012018111502"));

        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("科比"));     
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("age"), Bytes.toBytes("38"));      
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("weight"), Bytes.toBytes("85kg"));
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("sex"), Bytes.toBytes("男"));   

        table.put(put1);
        table.close();
    }


    /**
     * 数据的更新，hbase对数据只有追加，没有更新，但是查询的时候会把最新的数据返回给我们
     * 
     */
    @Test
    public void updateData() throws IOException {
        Table table = connection.getTable(TableName.valueOf("TABLE_1"));

        Put put1 = new Put(Bytes.toBytes("A0012018111502"));
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("科比"));

        table.put(put1);

        table.close();
        connection.close();
    }


    /**
     * 删除数据
     * 
     */
    @Test
    public void deleteData() throws IOException {
    	Table table = connection.getTable(TableName.valueOf("TABLE_1"));

        // 参数为 row key
        // 删除一列
        Delete delete1 = new Delete(Bytes.toBytes("A0012018111502"));
        delete1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("weight"));

        // 删除多列
        Delete delete2 = new Delete(Bytes.toBytes("A0012018111502"));
        delete2.addColumns(Bytes.toBytes("user"), Bytes.toBytes("age"));
        delete2.addColumns(Bytes.toBytes("user"), Bytes.toBytes("sex"));

        // 删除某一行的列族内容
        Delete delete3 = new Delete(Bytes.toBytes("A0012018111502"));
        delete3.addFamily(Bytes.toBytes("user"));

        // 删除一整行
        Delete delete4 = new Delete(Bytes.toBytes("A0012018111502"));

        // 执行 删除
        table.delete(delete1);
        table.delete(delete2);
        table.delete(delete3);
        table.delete(delete4);

        // 关闭表
        table.close();

        // 关闭连接
        connection.close();
    }


 
    /**
     * 查询
     * 
     */
    @Test
    public void querySingleRow() throws IOException {
        Table table = connection.getTable(TableName.valueOf("TABLE_1"));

        // 获得一行
        Get get = new Get(Bytes.toBytes("A0012018111502"));
        Result set = table.get(get);
        Cell[] cells  = set.rawCells();

        for(Cell cell : cells) {
            System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }

        table.close();
        //Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password")))

        connection.close();
    }


    /**
     * 全表扫描
     * 
     */
    @Test
    public void scanTable() throws IOException {
        Table table = connection.getTable(TableName.valueOf("TABLE_1"));
        Scan scan = new Scan();

        //scan.addFamily(Bytes.toBytes("info"));
        //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
        //scan.setStartRow(Bytes.toBytes("wangsf_0"));
        //scan.setStopRow(Bytes.toBytes("wangwu"));

        ResultScanner rsacn = table.getScanner(scan);

        for(Result rs:rsacn) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key : "+rowkey);
            Cell[] cells  = rs.rawCells();

            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }

            System.out.println("-----------------------------------------");
        }

        table.close();
        connection.close();
    }


    


    @After
    public void closeConn() throws IOException {
    	connection.close();
    }
    
    
    public void run() {

	}

}
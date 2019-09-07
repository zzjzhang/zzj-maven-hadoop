package task.hbase;

import org.junit.Test;
import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;


/**
 * 
 * HADOOP - 2.7.7
 * HBASE - 2.0.1
 * 
 * 过滤器
 * 
 * @author zsh10649
 *
 */
public class Task3 implements Runnable {

	// 1. 字段
	private Configuration configuration;
	private Connection connection;
	private Admin admin;
	private TableName tableName;
	
	
	// 2. 构造方法
	public Task3() {
		// 
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "192.168.157.129");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		// configuration.set("hbase.master", "10.88.27.136:9001");

		try {
			connection = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}

		tableName = TableName.valueOf("TABLE_1");
	}
	

	
	/**
     * 过滤器
     * 列值过滤器
     * 
     */
    public void singColumnFilter() throws IOException {
    	Table table = connection.getTable(TableName.valueOf("TABLE_1"));
        Scan scan = new Scan();

        // 下列参数分别为，列族，列名，比较符号，值
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("user"), Bytes.toBytes("name"), 
        		CompareOperator.EQUAL, Bytes.toBytes("科比"));
        scan.setFilter(filter);

        // 获取 返回结果
        ResultScanner scanner = table.getScanner(scan);

        for(Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key : " + rowkey);

            Cell[] cells  = rs.rawCells();

            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + " : " +
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }

            System.out.println("-----------------------------------------");
        }
    }


    /**
     * row key 过滤器
     * 
     */
    public void rowkeyFilter(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        RowFilter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("^hgs_00*"));

        scan.setFilter(filter);
        ResultScanner scanner  = table.getScanner(scan);

        for(Result result : scanner) {
            String rowkey = Bytes.toString(result.getRow());
            System.out.println("row key : " + rowkey);
            Cell[] cells  = result.rawCells();

            for(Cell cell : cells) {
                // System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) 
            	// + " : " + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));

            	String rowName = new String(CellUtil.cloneRow(cell), "UTF-8");              // 行名（即行键值）
				String columnFamily = new String(CellUtil.cloneFamily(cell), "UTF-8");      // 列族名
				String columnName = new String(CellUtil.cloneQualifier(cell), "UTF-8");     // 列名
				String columnValue = new String(CellUtil.cloneValue(cell), "UTF-8");        // 列值
            }

            System.out.println("-----------------------------------------");
        }
    }


    /**
     * 列名前缀过滤器
     * 
     * */
    public void columnPrefixFilter(String tableName, String prefix) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(prefix));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);

        for(Result result : scanner) {
            String rowkey = Bytes.toString(result.getRow());
            System.out.println("row key : " + rowkey);
            Cell[] cells = result.rawCells();

            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + " : " + 
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }

            System.out.println("-----------------------------------------");
        }
    }


    /**
     * 过滤器集合
     * 
     */
    public void filterSet(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        // 创建 过滤器 列表
        FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

        // 分别 设置 过滤器
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("user"),  Bytes.toBytes("age"), CompareOperator.GREATER, Bytes.toBytes("23"));
        ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("ag"));

        // 过滤器 集合 添加 过滤器
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);

        // 设置 过滤器
        scan.setFilter(filterList);

        // 获取 结果
        ResultScanner scanner = table.getScanner(scan);

        for(Result result : scanner) {
            String rowkey = Bytes.toString(result.getRow());
            System.out.println("row key : " + rowkey);
            Cell[] cells = result.rawCells();

            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }

            System.out.println("-----------------------------------------");
        }
    }
	
	
	
	
	
	
	
	
	
	
	
	
	public void run() {

	}

}

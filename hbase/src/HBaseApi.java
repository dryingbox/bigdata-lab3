package lab3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 实验三 第一部分：HBase Java API 编程。
 *
 * <p>本节实现题目要求的 5 个函数（方法签名与题目一致）：
 * <ol>
 *   <li>{@link #createTable(String, String[])}：建表，表已存在则先删除；fields 为列族名数组；</li>
 *   <li>{@link #addRecord(String, String, String[], String[])}：向指定行写入数据，
 *       fields 元素支持 "列族:列限定符" 形式（如 Score:Math）；</li>
 *   <li>{@link #scanColumn(String, String)}：浏览某一列族（列出其下所有列限定符）或某一具体列的数据，
 *       该行该列不存在时输出 null；</li>
 *   <li>{@link #modifyData(String, String, String)}：修改指定行、指定列单元格的数据
 *       （新值从标准输入读取，避免把测试数据写死在代码里）；</li>
 *   <li>{@link #deleteRow(String, String)}：删除指定行的整条记录。</li>
 * </ol>
 *
 * <p>表结构设计（把题目中的 3 张关系表迁移到 HBase）：
 * <pre>
 * Student  行键 = 姓名（S_Name），列族 Info，列 S_No / S_Sex / S_Age
 * Course   行键 = 课程号（C_No），列族 Info，列 C_Name / C_Credit
 * SC       行键 = 学生姓名（S_Name），列族 Score，列 Math / Computer Science / English
 * </pre>
 * 行键用姓名而不是学号，原因见 README（题目数据中 2015003 出现两次，用学号会互相覆盖）。
 *
 * <p>编译运行：
 * <pre>
 *   javac -cp "$(hbase classpath)" -d build src/HBaseApi.java
 *   echo 95 | java -cp "build:$(hbase classpath)" lab3.HBaseApi
 * </pre>
 */
public class HBaseApi {

    private static final Configuration CONF = HBaseConfiguration.create();

    // ------------------------------------------------------------------ 题目要求的 5 个函数

    /**
     * 创建表；若同名表已存在，先禁用并删除，再按 fields 中的列族新建。
     *
     * @param tableName 表名
     * @param fields    列族名称数组（如 {"Info"}、{"Score"}）
     */
    public static void createTable(String tableName, String[] fields) throws IOException {
        try (Connection conn = ConnectionFactory.createConnection(CONF);
             Admin admin = conn.getAdmin()) {
            TableName tn = TableName.valueOf(tableName);
            if (admin.tableExists(tn)) {
                admin.disableTable(tn);
                admin.deleteTable(tn);
                System.out.println("[createTable] 已存在同名表 " + tableName + "，已先删除");
            }
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tn);
            for (String family : fields) {
                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
            }
            admin.createTable(builder.build());
            System.out.println("[createTable] 创建表 " + tableName + "，列族 " + Arrays.toString(fields));
        }
    }

    /**
     * 向 tableName 表的 row 行写入数据。fields 元素为 "列族:列限定符" 或 "列族"，
     * 与 values 一一对应。
     */
    public static void addRecord(String tableName, String row, String[] fields, String[] values)
            throws IOException {
        if (fields.length != values.length) {
            throw new IllegalArgumentException("fields 与 values 长度不一致");
        }
        try (Connection conn = ConnectionFactory.createConnection(CONF);
             Table table = conn.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(row));
            for (int i = 0; i < fields.length; i++) {
                String[] fq = splitColumn(fields[i]);
                put.addColumn(Bytes.toBytes(fq[0]), Bytes.toBytes(fq[1]), Bytes.toBytes(values[i]));
            }
            table.put(put);
            System.out.println("[addRecord] " + tableName + " 行 " + row + " 写入 " + fields.length + " 个单元格");
        }
    }

    /**
     * 浏览 tableName 表 column 列的数据：
     * <ul>
     *   <li>column 为列族名（如 Score）：列出该列族下每个列限定符的数据；</li>
     *   <li>column 为具体列（如 Score:Math）：只列出该列的数据；</li>
     *   <li>某行该列不存在时输出 null（题目要求“返回 null”）。</li>
     * </ul>
     */
    public static void scanColumn(String tableName, String column) throws IOException {
        String[] fq = splitColumn(column);
        byte[] family = Bytes.toBytes(fq[0]);
        boolean familyOnly = fq[1].isEmpty();
        try (Connection conn = ConnectionFactory.createConnection(CONF);
             Table table = conn.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            scan.addFamily(family);
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    String rowKey = Bytes.toString(result.getRow());
                    if (familyOnly) {
                        boolean printed = false;
                        for (Cell cell : result.rawCells()) {
                            if (!Bytes.equals(CellUtil.cloneFamily(cell), family)) {
                                continue;
                            }
                            System.out.println(rowKey + "\t"
                                    + Bytes.toString(CellUtil.cloneQualifier(cell)) + "="
                                    + Bytes.toString(CellUtil.cloneValue(cell)));
                            printed = true;
                        }
                        if (!printed) {
                            System.out.println(rowKey + "\tnull");
                        }
                    } else {
                        byte[] value = result.getValue(family, Bytes.toBytes(fq[1]));
                        System.out.println(rowKey + "\t" + (value == null ? "null" : Bytes.toString(value)));
                    }
                }
            }
        }
    }

    /**
     * 修改 tableName 表 row 行 column 列单元格的数据。新值从标准输入读取一行；
     * 若输入为空或流已结束，则不修改并给出提示。
     */
    public static void modifyData(String tableName, String row, String column) throws IOException {
        System.out.print("[modifyData] 请输入 " + tableName + " 表 " + row + " 行 " + column + " 列的新值: ");
        System.out.flush();
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String newValue = in.readLine();
        if (newValue == null || newValue.trim().isEmpty()) {
            System.out.println("[modifyData] 未读取到新值，放弃修改");
            return;
        }
        String[] fq = splitColumn(column);
        try (Connection conn = ConnectionFactory.createConnection(CONF);
             Table table = conn.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(fq[0]), Bytes.toBytes(fq[1]), Bytes.toBytes(newValue.trim()));
            table.put(put);
            System.out.println("[modifyData] " + row + " 的 " + column + " 已改为 " + newValue.trim());
        }
    }

    /** 删除 tableName 表 row 行（整条记录）。 */
    public static void deleteRow(String tableName, String row) throws IOException {
        try (Connection conn = ConnectionFactory.createConnection(CONF);
             Table table = conn.getTable(TableName.valueOf(tableName))) {
            table.delete(new Delete(Bytes.toBytes(row)));
            System.out.println("[deleteRow] 已删除 " + tableName + " 表的行 " + row);
        }
    }

    // ------------------------------------------------------------------ 辅助方法

    /** "Score:Math" -> {"Score", "Math"}；"Score" -> {"Score", ""}。 */
    private static String[] splitColumn(String column) {
        int idx = column.indexOf(':');
        if (idx < 0) {
            return new String[] {column, ""};
        }
        return new String[] {column.substring(0, idx), column.substring(idx + 1)};
    }

    /** 打印整张表，便于核对。</br> */
    public static void scanTable(String tableName) throws IOException {
        try (Connection conn = ConnectionFactory.createConnection(CONF);
             Table table = conn.getTable(TableName.valueOf(tableName));
             ResultScanner scanner = table.getScanner(new Scan())) {
            int rows = 0;
            for (Result result : scanner) {
                rows++;
                StringBuilder sb = new StringBuilder(Bytes.toString(result.getRow())).append(":");
                for (Cell cell : result.rawCells()) {
                    sb.append(' ').append(Bytes.toString(CellUtil.cloneFamily(cell))).append(':')
                            .append(Bytes.toString(CellUtil.cloneQualifier(cell))).append('=')
                            .append(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                System.out.println("  " + sb);
            }
            System.out.println("  共 " + rows + " 行");
        }
    }

    /** 列出 HBase 所有表名。 */
    public static void listTables() throws IOException {
        try (Connection conn = ConnectionFactory.createConnection(CONF);
             Admin admin = conn.getAdmin()) {
            TableName[] tables = admin.listTableNames();
            System.out.println("[listTables] HBase 中共有 " + tables.length + " 张表: " + Arrays.toString(tables));
        }
    }

    // ------------------------------------------------------------------ 演示主流程

    public static void main(String[] args) throws Exception {
        System.out.println("================ 1. createTable：建立 Student / Course / SC ================");
        createTable("Student", new String[] {"Info"});
        createTable("Course", new String[] {"Info"});
        createTable("SC", new String[] {"Score"});

        System.out.println("================ 2. addRecord：插入学生表 / 课程表 / 选课表 ================");
        addRecord("Student", "Zhangsan", new String[] {"Info:S_No", "Info:S_Sex", "Info:S_Age"},
                new String[] {"2015001", "male", "23"});
        addRecord("Student", "Mary", new String[] {"Info:S_No", "Info:S_Sex", "Info:S_Age"},
                new String[] {"2015002", "female", "22"});
        addRecord("Student", "Lisi", new String[] {"Info:S_No", "Info:S_Sex", "Info:S_Age"},
                new String[] {"2015003", "male", "24"});

        addRecord("Course", "123001", new String[] {"Info:C_Name", "Info:C_Credit"},
                new String[] {"Math", "2.0"});
        addRecord("Course", "123002", new String[] {"Info:C_Name", "Info:C_Credit"},
                new String[] {"Computer Science", "5.0"});
        addRecord("Course", "123003", new String[] {"Info:C_Name", "Info:C_Credit"},
                new String[] {"English", "3.0"});

        addRecord("SC", "Zhangsan", new String[] {"Score:Math", "Score:English"},
                new String[] {"86", "69"});
        addRecord("SC", "Mary", new String[] {"Score:Computer Science", "Score:English"},
                new String[] {"77", "99"});
        addRecord("SC", "Lisi", new String[] {"Score:Math", "Score:Computer Science"},
                new String[] {"98", "95"});

        System.out.println("================ 3. scanColumn：按列族 / 按具体列浏览 ================");
        System.out.println("-- scanColumn(SC, \"Score\") --");
        scanColumn("SC", "Score");
        System.out.println("-- scanColumn(SC, \"Score:Math\") --");
        scanColumn("SC", "Score:Math");
        System.out.println("-- scanColumn(SC, \"Score:Physics\")（不存在，应输出 null） --");
        scanColumn("SC", "Score:Physics");

        System.out.println("================ 4. modifyData：修改 Zhangsan 的 Math 成绩 ================");
        modifyData("SC", "Zhangsan", "Score:Math");
        System.out.println("-- 修改后 SC 表 --");
        scanTable("SC");

        System.out.println("================ 5. deleteRow：删除 Lisi 的行，再恢复 ================");
        deleteRow("SC", "Lisi");
        System.out.println("-- 删除后 SC 表 --");
        scanTable("SC");
        addRecord("SC", "Lisi", new String[] {"Score:Math", "Score:Computer Science"},
                new String[] {"98", "95"});
        System.out.println("-- 恢复后 SC 表 --");
        scanTable("SC");

        System.out.println("================ Student / Course 表内容 ================");
        scanTable("Student");
        scanTable("Course");
        listTables();
    }
}

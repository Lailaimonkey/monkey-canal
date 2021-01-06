package canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.Message;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: LailaiMonkey
 * @Description：
 * @Date：Created in 2021-01-04 11:53
 * @Modified By：
 */
public class SimpleCanalClientExample {

    static Statement statement = null;

    public static void main(String args[]) {

        //获得存储sqlStatement
        getStatement();

        // 创建Canal链接
        CanalConnector connector = getCanalConnector();
        int batchSize = 10000;
        while (true) {
            // 获取指定数量的数据
            Message message = connector.getWithoutAck(batchSize);

            long batchId = message.getId();
            int size = message.getEntries().size();
            if (batchId == -1 || size == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            } else {
                findEntry(message.getEntries());
            }

            connector.ack(batchId); // 提交确认
            // connector.rollback(batchId); // 处理失败, 回滚数据
        }
    }

    private static CanalConnector getCanalConnector() {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(),
                11111), "example", "", "");
        connector.connect();
        connector.subscribe(".*\\..*");
        connector.rollback();
        return connector;
    }

    private static void getStatement() {
        //驱动程序名
        String driver = "com.mysql.cj.jdbc.Driver";
        // URL指向要访问的数据库名scutcs
        String url = "jdbc:mysql://从库IP:3306/数据库?useLegacyDatetimeCode=false&serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
        // MySQL配置时的用户名
        String user = "账号";
        // Java连接MySQL配置时的密码
        String password = "密码";

        try {
            // 加载驱动程序
            Class.forName(driver);

            // 连续数据库
            Connection conn = DriverManager.getConnection(url, user, password);
            if (!conn.isClosed()) {
                System.out.println("Succeeded connecting to the Database!");
            }

            // statement用来执行SQL语句
            statement = conn.createStatement();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static void findEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    String sql = "delete from " + entry.getHeader().getTableName();
                    sql += deleteSql(rowData.getBeforeColumnsList());
                    doSql(sql);
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    String sql = "replace into " + entry.getHeader().getTableName();
                    sql += insertSql(rowData.getAfterColumnsList());
                    doSql(sql);
                } else {
                    String sql = "update " + entry.getHeader().getTableName();
                    sql += updateSql(rowData.getAfterColumnsList());
                    doSql(sql);
                }
            }
        }
    }

    private static String updateSql(List<Column> columns) {
        StringBuilder str = new StringBuilder(" set ");

        CanalField canalField = getField(columns);
        List<String> fields = canalField.getFields();
        List<String> values = canalField.getValues();

        for (int i = 0; i < fields.size(); i++) {
            str.append(fields.get(i)).append(" = ").append(values.get(i)).append(",");
        }

        str.deleteCharAt(str.length() - 1);
        //以主键为条件更新
        str.append(" where id = ").append(canalField.getId());
        return str.toString();
    }

    private static String insertSql(List<Column> columns) {
        StringBuilder str = new StringBuilder(" ( ");
        CanalField canalField = getField(columns);
        List<String> fields = canalField.getFields();
        List<String> values = canalField.getValues();

        str.append(String.join(",", fields))
                .append(") values (")
                .append(String.join(",", values))
                .append(" ) ");
        return str.toString();
    }

    private static String deleteSql(List<Column> columns) {
        StringBuilder str = new StringBuilder(" where id = ");
        String id = "";
        for (Column column : columns) {
            if ("id".equals(column.getName())) {
                id = column.getValue();
            }
        }
        str.append(id);
        return str.toString();
    }

    private static CanalField getField(List<Column> columns) {
        CanalField canalField = new CanalField();

        List<String> field = new ArrayList<>();
        List<String> value = new ArrayList<>();
        for (Column column : columns) {
            if (StringUtils.isEmpty(column.getValue())) {
                continue;
            }

            //获得id（主键）字段值
            if ("id".equals(column.getName())) {
                canalField.setId(column.getValue());
            }

            //获得字段和值
            int sqlType = column.getSqlType();
            if (sqlType <= 8 && sqlType >= 2 || sqlType <= -5 && sqlType >= -7) {
                field.add("`" + column.getName() + "`");
                value.add(column.getValue());
            } else {
                field.add("`" + column.getName() + "`");
                value.add("'" + column.getValue() + "'");
            }
        }

        canalField.setFields(field);
        canalField.setValues(value);
        return canalField;
    }

    private static void doSql(String sql) {
        try {
            // 要执行的SQL语句
            statement.executeUpdate(sql);
        } catch (Exception e) {
            System.out.println("执行失败：" + sql);
            e.printStackTrace();
        }
    }

}

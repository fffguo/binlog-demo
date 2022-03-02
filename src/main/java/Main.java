import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ListIterator;

/**
 * @author lfg
 * @version 1.0
 * Main
 */
public class Main {

    public static void main(String[] args) throws IOException {
        BinaryLogClient client = new BinaryLogClient("127.0.0.1", 3306, "root", "123456");
        EventDeserializer eventDeserializer = new EventDeserializer();
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(event -> {
            try {
                onEvent(event);
            } catch (SQLException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        });
        client.connect();
    }


    private static void onEvent(Event event) throws SQLException, ClassNotFoundException {
        if (EventType.EXT_WRITE_ROWS.equals(event.getHeader().getEventType())) {
            final JSONObject data = JSONObject.parseObject(JSON.toJSONString(event.getData()));
            if ("123".equals(data.getString("tableId"))) {
                System.out.println("data: " + data);
                final ListIterator<Object> rows = data.getJSONArray("rows").listIterator();
                while (rows.hasNext()) {
                    final JSONArray row = JSONArray.parseArray(JSONObject.toJSONString(rows.next()));
                    final Object id = row.get(0);
                    final Object name = row.get(1);
                    insert((Integer) id, (String) name);
                }
            }
        }

    }

    public static void insert(Integer id, String name) throws ClassNotFoundException, SQLException {
        // 1.注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        // 2.获取连接对象
        String url = "jdbc:mysql://127.0.0.1:3306/test_database?characterEncoding=utf-8";
        Connection conn = DriverManager.getConnection(url, "root", "123456");
        // 3.获取执行SQL语句
        Statement stat = conn.createStatement();
        // 拼写SQL语句
        String sql = String.format("INSERT INTO binlog_target (id, name) VALUES (%s, '%s');", id, name);
        System.out.println("sql: " + sql + "\n");
        // 4.调用执行者对象方法,执行SQL语句获取结果集
        stat.execute(sql);
        stat.close();
        conn.close();
    }
}

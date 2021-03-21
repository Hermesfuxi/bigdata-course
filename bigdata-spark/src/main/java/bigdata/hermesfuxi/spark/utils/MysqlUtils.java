package bigdata.hermesfuxi.spark.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ResourceBundle;

/**
 * @author Hermesfuxi
 */
public class MysqlUtils {
    private static String url;
    private static String user;
    private static String password;
    static {
        ResourceBundle resourceBundle = ResourceBundle.getBundle("mysql");
        url = resourceBundle.getString("url");
        user = resourceBundle.getString("user");
        password = resourceBundle.getString("password");
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}

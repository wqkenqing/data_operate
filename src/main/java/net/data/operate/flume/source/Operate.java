package net.data.operate.flume.source;

import java.sql.*;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-09-03
 * @desc
 */
public class Operate {
    public  Connection initConfig() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        Connection con= DriverManager.getConnection(
                "jdbc:mysql://211.159.172.76:3306/solo","root","125323Wkq");
        return con;
    }


    public ResultSet  selectJob() throws SQLException, ClassNotFoundException {
        Connection con = initConfig();
        String sql = "select * from b3_solo_article";
        Statement statement = con.createStatement();
        ResultSet res = statement.executeQuery(sql);
        return res;
    }



    public static void main(String[] args) {
        Operate operate = new Operate();
        try {
            operate.selectJob();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}

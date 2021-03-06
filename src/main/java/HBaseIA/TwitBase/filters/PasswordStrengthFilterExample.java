package HBaseIA.TwitBase.filters;

import HBaseIA.TwitBase.hbase.UsersDAO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import utils.Const;

import java.io.IOException;


public class PasswordStrengthFilterExample {

  public static void main (String[] args) {
    try {
      Configuration configuration = new Configuration();
      configuration.set("hbase.zookeeper.quorum", Const.ZK_QUORUM);
      configuration.set("hbase.zookeeper.property.clientPort", Const.ZK_PORT);

      Connection connection = ConnectionFactory.createConnection(configuration);
      Table table = connection.getTable(UsersDAO.TABLE_NAME);
      Scan scan = new Scan();
      scan.addColumn(UsersDAO.INFO_FAM, UsersDAO.PASS_COL);
      scan.addColumn(UsersDAO.INFO_FAM, UsersDAO.NAME_COL);
      scan.addColumn(UsersDAO.INFO_FAM, UsersDAO.EMAIL_COL);
      Filter f = new PasswordStrengthFilter(4);
      scan.setFilter(f);
      ResultScanner rs = table.getScanner(scan);
      for (Result r : rs) {
        System.out.println(r);
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}

package HBaseIA.TwitBase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import HBaseIA.TwitBase.hbase.TwitsDAO;
import HBaseIA.TwitBase.hbase.UsersDAO;
import HBaseIA.TwitBase.model.Twit;
import utils.Const;

public class TwitsTool {

  private static final Logger log = Logger.getLogger(TwitsTool.class);

  private static final String usage =
    "twitstool action ...\n" +
    "  help - print this message and exit.\n" +
    "  post user text - post a new twit on user's behalf.\n" +
    "  list user - list all twits for the specified user.\n";

  public static void main(String[] args) throws IOException {
    if (args.length == 0 || "help".equals(args[0])) {
      System.out.println(usage);
      System.exit(0);
    }

    Configuration configuration = new Configuration();
    configuration.set("hbase.zookeeper.quorum", Const.ZK_QUORUM);
    configuration.set("hbase.zookeeper.property.clientPort", Const.ZK_PORT);

    Connection connection = ConnectionFactory.createConnection(configuration);

    TwitsDAO twitsDao = new TwitsDAO(connection);
    UsersDAO usersDao = new UsersDAO(connection);

    if ("post".equals(args[0])) {
      DateTime now = new DateTime();
      log.debug("Posting twit at ..." + now);
      twitsDao.postTwit(args[1], now, args[2]);
      Twit t = twitsDao.getTwit(args[1], now);
      usersDao.incTweetCount(args[1]);
      System.out.println("Successfully posted " + t);
    }

    if ("list".equals(args[0])) {
      List<Twit> twits = twitsDao.list(args[1]);
      log.info("Found %s twits." +twits.size());
      for(Twit t : twits) {
        System.out.println(t);
      }
    }

    connection.close();
  }
}

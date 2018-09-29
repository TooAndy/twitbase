package HBaseIA.TwitBase.coprocessors;

import static HBaseIA.TwitBase.hbase.RelationsDAO.FOLLOWS_TABLE_NAME;
import static HBaseIA.TwitBase.hbase.RelationsDAO.FROM;
import static HBaseIA.TwitBase.hbase.RelationsDAO.RELATION_FAM;
import static HBaseIA.TwitBase.hbase.RelationsDAO.TO;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import HBaseIA.TwitBase.hbase.RelationsDAO;
import utils.Const;

public class FollowsObserver extends BaseRegionObserver {

  private Connection connection = null;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    Configuration configuration = new Configuration();
    configuration.set("hbase.zookeeper.quorum", Const.ZK_QUORUM);
    configuration.set("hbase.zookeeper.property.clientPort", Const.ZK_PORT);
    connection = ConnectionFactory.createConnection(configuration);
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    connection.close();
  }

  @Override
  public void postPut(
      final ObserverContext<RegionCoprocessorEnvironment> e,
      final Put put,
      final WALEdit edit,
      final Durability durability)
    throws IOException {

    byte[] table
      = e.getEnvironment().getRegion().getRegionInfo().getTableName();
    if (!Bytes.equals(table, FOLLOWS_TABLE_NAME))
      return;

    Cell cell = put.get(RELATION_FAM, FROM).get(0);
    String from = Bytes.toString(CellUtil.cloneValue(cell));
    cell = put.get(RELATION_FAM, TO).get(0);
    String to = Bytes.toString(CellUtil.cloneValue(cell));

    RelationsDAO relations = new RelationsDAO(connection);
    relations.addFollowedBy(to, from);
  }
}

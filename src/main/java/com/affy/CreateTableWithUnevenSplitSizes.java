package com.affy;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public class CreateTableWithUnevenSplitSizes {

    public static void main(String[] args) throws IOException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {

        Properties prop = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream in = loader.getResourceAsStream("accumulo.properties");
        prop.load(in);

        String user = prop.getProperty("accumulo.user");
        String password = prop.getProperty("accumulo.password");
        String instanceInfo = prop.getProperty("accumulo.instance");
        String zookeepers = prop.getProperty("accumulo.zookeepers");

        Instance instance = new ZooKeeperInstance(instanceInfo, zookeepers);

        Connector connector = instance.getConnector(user, new PasswordToken(password));

        for (int tableCounter = 0; tableCounter < 25; tableCounter++) {

            String accumuloTable = "table" + String.format("%04d", tableCounter);
            
            System.out.println("Creating " + accumuloTable);

            if (false == connector.tableOperations().exists(accumuloTable)) {
                connector.tableOperations().create(accumuloTable);
            }

            SortedSet<Text> splits = new TreeSet<Text>();
            splits.add(new Text("0"));
            splits.add(new Text("1"));
            splits.add(new Text("2"));
            splits.add(new Text("3"));
            splits.add(new Text("4"));

            connector.tableOperations().addSplits(accumuloTable, splits);

            Text cf = new Text("");
            Text cq = new Text("");
            Value empty = new Value("".getBytes());

            BatchWriter writer = connector.createBatchWriter(accumuloTable, new BatchWriterConfig());
            for (int i = 0; i < 1000000; i++) {
                String row = String.format("%04d", i);
                Mutation m = new Mutation(new Text(row));
                m.put(cf, cq, empty);
                writer.addMutation(m);
            }
            for (int i = 0; i < 10000; i++) {
                String row = String.format("1%04d", i);
                Mutation m = new Mutation(new Text(row));
                m.put(cf, cq, empty);
                writer.addMutation(m);
            }
            for (int i = 0; i < 10000; i++) {
                String row = String.format("2%04d", i);
                Mutation m = new Mutation(new Text(row));
                m.put(cf, cq, empty);
                writer.addMutation(m);
            }
            for (int i = 0; i < 10000; i++) {
                String row = String.format("3%04d", i);
                Mutation m = new Mutation(new Text(row));
                m.put(cf, cq, empty);
                writer.addMutation(m);
            }
            for (int i = 0; i < 25000; i++) {
                String row = String.format("4%04d", i);
                Mutation m = new Mutation(new Text(row));
                m.put(cf, cq, empty);
                writer.addMutation(m);
            }
            writer.close();

            connector.tableOperations().compact(accumuloTable, null, null, true, true);
        }

    }
}

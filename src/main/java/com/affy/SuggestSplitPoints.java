package com.affy;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A map reduce job that suggests split points for tables that have more
 * than the average number of entries.
 */
public class SuggestSplitPoints extends Configured implements Tool {

    private static final Text EMPTY = new Text();
    public static final String AVERAGE_NUMBER_OF_ENTRIES = "AVERAGE_NUMBER_OF_ENTRIES";
    public static final String MINIMUM_NUMBER_OF_ENTRIES = "MINIMUM_NUMBER_OF_ENTRIES";

    public static class UMapper extends Mapper<Key, Value, Text, Text> {

        private Text temp = new Text();
        private int count = 0;
        private int averageCount = 0;
        private int minCount = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            averageCount = Integer.parseInt(context.getConfiguration().get(AVERAGE_NUMBER_OF_ENTRIES));
            minCount = Integer.parseInt(context.getConfiguration().get(MINIMUM_NUMBER_OF_ENTRIES));
        }

        @Override
        public void map(Key key, Value value, Context context) throws IOException, InterruptedException {
            count++;
            if (count > minCount && count > averageCount) {
                context.write(key.getRow(), EMPTY);
                count = 0;
            }
        }
    }

    static class Opts extends ClientOnRequiredTable {
        @Parameter(names = "--output", description = "output directory")
        String output;
        @Parameter(names = "--offline", description = "run against an offline table")
        boolean offline = false;
        @Parameter(names = "--min_entries", description = "minimum number of entries per tablet.")
        String minEntries;
    }

    @Override
    public int run(String[] args) throws Exception {
        Opts opts = new Opts();
        opts.parseArgs(SuggestSplitPoints.class.getName(), args);

        Connector conn = opts.getConnector();
        
        // Find the average number of entries per tablet.
        Instance instance = opts.getInstance();
        String tableId = Tables.getNameToIdMap(instance).get(opts.tableName);
        int numEntries = 0;
        int numSplits = 0;

        Scanner scanner = new IsolatedScanner(conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS));
        scanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
        scanner.setRange(new KeyExtent(new Text(tableId), null, null).toMetadataRange());
        for (Map.Entry<Key, Value> entry : scanner) {
            String value = entry.getValue().toString();
            String[] components = value.split(",");
            numEntries += Integer.parseInt(components[1]);
            numSplits++;
            System.out.println(String.format("%,d: %,d", numSplits, numEntries));
        }
        int average = numEntries / numSplits;
        
        String jobName = this.getClass().getSimpleName() + "_" + System.currentTimeMillis();
        
        FileSystem.get(getConf()).delete(new Path(opts.output), true);

        Job job = new Job(getConf(), jobName);
        job.setJarByClass(this.getClass());
        job.getConfiguration().set(AVERAGE_NUMBER_OF_ENTRIES, Integer.toString(average));
        job.getConfiguration().set(MINIMUM_NUMBER_OF_ENTRIES, opts.minEntries);

        String clone = opts.tableName;
        if (opts.offline) {
            clone = opts.tableName + "_" + jobName;
            conn.tableOperations().clone(opts.tableName, clone, true, new HashMap<String, String>(), new HashSet<String>());
            conn.tableOperations().offline(clone);

            AccumuloInputFormat.setOfflineTableScan(job, true);
        }
        
        job.setInputFormatClass(AccumuloInputFormat.class);
        opts.setAccumuloConfigs(job);

        job.setMapperClass(UMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(opts.output));

        job.waitForCompletion(true);

        if (opts.offline) {
            conn.tableOperations().delete(clone);
        }

        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(CachedConfiguration.getInstance(), new SuggestSplitPoints(), args);
        System.exit(res);
    }
}
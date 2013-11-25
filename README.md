The project suggests split points for Accumulo tablets based on the average
number of entries per tablet. The idea behind this project is to avoid uneven
processing times between tablets.

1. Start an Accumulo cluster using https://github.com/medined/Accumulo_1_5_0_By_Vagrant.
2. Download this project (I used the host machine, not the virtual one).
3. Run 'mvn package' to generate a jar file.
4. Copy the jar file to the Accumulo_1_5_0_By_Vagrant directory.
5. Switch to the Accumulo_1_5_0_By_Vagrant directory.
6. vagrant ssh master
7. Run the following command making sure to change the table name to something
that exists.

```
tool.sh \
  /vagrant/SplitLargeTablets-1.0-SNAPSHOT.jar \
  com.affy.SuggestSplitPoints \
  -i instance \
  -u root \
  -p secret \
  -z affy-master:2181 \
  --output ./suggest_split_points \
  --min_entries 1000000 \
  -t tableA
```

8. hadoop fs -ls ./suggest_split_points

The split point suggestions are in any file with non-zero length. It's fairly easy to
read the set of part-m-XXXX file to build a SortSet which can be passed to
the addSplits method of TableOperations.

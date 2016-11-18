import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Shuffle<K, V> {
  final Partitioner<K, V> partitioner;
  final int numTaskIndexs;
  final Map<Integer , List<KV<K, V>>> intermOutputMap = new ConcurrentHashMap<>(); // intermediate output map

  Shuffle(final Partitioner<K, V> partitioner, int numTaskIndexs) {
    this.partitioner = partitioner;
    this.numTaskIndexs = numTaskIndexs;
  }

  public synchronized void write(final Iterator<KV<K, V>> taskOutput) throws IOException {
    // TODO: Save the data
    Map<Integer, List<KV<K, V>>> partitions = partitioner.partition(taskOutput, numTaskIndexs);
    Set<Integer> keySet = partitions.keySet();
    for (Integer taskIndex: keySet) {
      final List<KV<K, V>> kvList = partitions.get(taskIndex);
      if (!intermOutputMap.containsKey(taskIndex)) {
        intermOutputMap.put(taskIndex, kvList);
      } else {
        intermOutputMap.get(taskIndex).addAll(kvList);
      }
    }
  }

  public synchronized Iterator<KV<K, V>> read(final int taskIndex) {
    // TODO: Pull the saved data
    return intermOutputMap.get(taskIndex).iterator();
  }
}

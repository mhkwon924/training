import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HashPartitioner<K, V> implements Partitioner<K, V> {
  /**
   * @param keyValues iterator(key, value)
   * @return (destinationIndex -> iterator(key, value))
   */
  @Override
  public Map<Integer, List<KV<K, V>>> partition(Iterator<KV<K, V>> keyValues, int numPartitions) {
    Map<Integer, List<KV<K, V>>> partitionMaps = new ConcurrentHashMap<>();
    // TODO: Perform Hash-partitioning
    while (keyValues.hasNext()) {
      KV<K, V> keyValue = keyValues.next();
      K key = keyValue.getKey();
      Integer destIndex = key.toString().hashCode() % numPartitions;
      if (!partitionMaps.containsKey(destIndex)) {
        partitionMaps.put(destIndex, new ArrayList<>());
      }

      partitionMaps.get(destIndex).add(keyValue);
    }

    return partitionMaps;
  }
}

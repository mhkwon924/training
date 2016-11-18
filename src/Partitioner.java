import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface Partitioner<K, V> {
  Map<Integer, List<KV<K, V>>> partition(Iterator<KV<K, V>> keyValues, int numPartitions);
}

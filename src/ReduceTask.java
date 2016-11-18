import java.util.Iterator;

public class ReduceTask implements Task<KV<String, Iterator<Integer>>, KV<String, Integer>> {
  /**
   * @param groupedValues (name, count1, count2, ...)
   * @return (name, sumOfCounts)
   */
  @Override
  public KV<String, Integer> compute(KV<String, Iterator<Integer>> groupedValues) {
    // TODO: Compute the sum of groupedValues
    Integer sumValues = new Integer(0);
    Iterator<Integer> values =groupedValues.getValue();

    while(values.hasNext()) {
      sumValues += values.next();
    }

    return new KV<>(groupedValues.getKey(), sumValues);
  }
}

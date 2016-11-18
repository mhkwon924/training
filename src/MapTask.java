import java.lang.String;

public class MapTask implements Task<String, KV<String, Integer>> {
  /**
   * @param line "name count"
   * @return (name, count)
   */
  @Override
  public KV<String, Integer> compute(final String line) {
    // TODO: Parse a line into a Key-Value pair
		String delims = "[ ]+";
		String[] tokens = line.split(delims);
    return new KV<>(tokens[0], Integer.parseInt(tokens[1]));
  }
}

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Engine {
  final ExecutorService executors = Executors.newFixedThreadPool(2);
  final String inputDirectory;
  final int numMapTask;
  final int numReduceTask;
  final String outputDirectory;

  Engine(final String inputDirectory,
         final int numMapTask,
         final int numReduceTask,
         final String outputDirectory) {
    this.inputDirectory = inputDirectory;
    this.numMapTask = numMapTask;
    this.numReduceTask = numReduceTask;
    this.outputDirectory = outputDirectory;
  }

  void runJob() throws InterruptedException {
    // Shuffle Configuration
    final Shuffle<String, Integer> shuffle = new Shuffle<>(new HashPartitioner<String, Integer>(), numReduceTask);

    // Map
    final CountDownLatch mapWait = new CountDownLatch(numMapTask);
    for (int i = 0 ; i < numMapTask; i ++) {
      final int index = i;
      executors.execute(() -> {
        final MapTask mapTask = new MapTask();
        final List<KV<String, Integer>> kvList = new ArrayList<>();

        // TODO: Read inputFile -> Execute Map Task -> Perform Shuffle Write
        try {
          final BufferedReader reader = new BufferedReader(new FileReader(inputDirectory + '/' + index));
          for (String data = reader.readLine(); data != null; data = reader.readLine()){
            kvList.add(mapTask.compute(data));
          }

          shuffle.write(kvList.iterator());
          mapWait.countDown();
        } catch (Exception e) {
          // ignore missing input files.
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });
    }

    mapWait.await();

    // Reduce
    final CountDownLatch reduceWait = new CountDownLatch(numReduceTask);
    for (int i = 0 ; i < numReduceTask; i++) {
      final int index = i;
      executors.execute(() -> {
        ReduceTask reduceTask = new ReduceTask();
        final Map<String, List<Integer>> kvMap = new ConcurrentHashMap<>();
        // TODO: Perform Shuffle Read -> Execute Reduce Task -> Write outputFile
        // try (final BufferedWriter writer = new BufferedWriter(new FileWriter(outputDirectory + '/' + index))) {
        try {

          // what if there is no file associated with the given task index?
          final Iterator<KV<String, Integer>> inputIter = shuffle.read(index);
          while(inputIter.hasNext()) {
            KV<String, Integer> keyValue = inputIter.next();
            String key = keyValue.getKey();
            if (kvMap.containsKey(keyValue.getKey())) {
              kvMap.get(key).add(keyValue.getValue());
            } else {
              List<Integer> values = new ArrayList<>();
              values.add(keyValue.getValue());
              kvMap.put(key, values);
            }
          }

          final BufferedWriter writer = new BufferedWriter(new FileWriter(outputDirectory + '/' + index));
          for (String key : kvMap.keySet()) {
            writer.write(reduceTask.compute(new KV<>(key, kvMap.get(key).iterator())).toString() + "\n");
          }
          writer.close();

        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } finally {
          reduceWait.countDown();
        }
      });
    }
    reduceWait.await();
  }

  public static void main(final String[] args) throws IOException, InterruptedException {
    new Engine(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]), args[3]).runJob();
    System.exit(0);
  }
}

package com.orientechnologies.crashtest;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import java.lang.management.ManagementFactory;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.orientechnologies.crashtest.DataLoader.*;

class DataChecker {

  private static final CrashFlag CRASH_FLAG_M_BEAN = new CrashFlag();
  private static final String ONLY_CHECK_FLAG = "-onlyCheck";

  private static final Logger logger = LogManager.getLogger(DataChecker.class);

  static {
    OGlobalConfiguration.STORAGE_CHECKSUM_MODE.setValue(OChecksumMode.StoreAndThrow);
  }

  public static void main(String[] args) {
    if (args.length > 0 && args[0].equals(ONLY_CHECK_FLAG)) {
      logger.info("Perform db check only");

      final String dbPath = args[1];
      final String dbName = args[2];

      final Set<String> argSet = new HashSet<>(Arrays.asList(args).subList(3, args.length));

      boolean addIndexes = false;
      boolean addBinaryRecords = false;

      if (argSet.contains(DataLoader.ADD_INDEX_FLAG)) {
        addIndexes = true;
      }

      if (argSet.contains(DataLoader.ADD_BINARY_RECORDS_FLAG)) {
        addBinaryRecords = true;
      }

      executeDbCheckOnly(dbPath, dbName, addIndexes, addBinaryRecords);
    } else {
      executeCrashSuite();
    }
  }

  private static void executeDbCheckOnly(final String dbPath, final String dbName,
      final boolean addIndexes,
      final boolean addBinaryRecords) {

    try (OrientDB orientDB = new OrientDB("plocal:" + dbPath, OrientDBConfig.defaultConfig())) {
      runDbCheck(dbName, addBinaryRecords, addIndexes, orientDB);
    }

  }

  private static void executeCrashSuite() {
    try {
      ManagementFactory.getPlatformMBeanServer().registerMBean(
          CRASH_FLAG_M_BEAN,
          new ObjectName("com.orientechnologies.crashtest:type=CrashTestFlag")
      );
    } catch (InstanceAlreadyExistsException | MBeanRegistrationException |
             NotCompliantMBeanException | MalformedObjectNameException e) {
      throw new RuntimeException(e);
    }

    logger.info("Crash suite is started");
    final long timeSeed = System.nanoTime();
    logger.info("TimeSeed: {}", timeSeed);
    final Random random = new Random(timeSeed);

    final Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, 7);
    final long endTime = calendar.getTimeInMillis();

    final Path stopFilePath = Paths.get("target/stop.txt");

    int counter = 0;
    try {
      while (System.currentTimeMillis() < endTime) {
        if (stopFilePath.toFile().exists()) {
          logger.info("Crash suite is stopped by stop file");
          return;
        }

        counter++;
        logger.info("Crash test is started, {} iteration", counter);

        final boolean addIndex = random.nextBoolean();
        final boolean addBinaryRecords = random.nextBoolean();
        final boolean useSmallDiskCache = random.nextBoolean();
        final boolean useSmallWal = random.nextBoolean();
        final boolean generateOOM = false;

        if (startAndCrash(random, addIndex, addBinaryRecords, useSmallDiskCache, useSmallWal,
            generateOOM)) {
          logger.info("Wait for 1 min to be sure that all file locks are released");
          //noinspection BusyWait
          Thread.sleep(60 * 1000);

          logger.info("DB size is {} mb",
              calculateDirectorySize(DATABASES_PATH + File.separator + DB_NAME) / (1024 * 1024));

          checkDatabase(addIndex, addBinaryRecords);
        } else {
          return;
        }
        logger.info("Crash test is completed");
      }
    } catch (Exception e) {
      logger.error("Error during crash test execution", e);
      CRASH_FLAG_M_BEAN.setCrashDetected(true);
      try {
        Thread.sleep(5 * 60 * 1_000);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }

    logger.info("Crash suite is completed");
  }

  @SuppressWarnings("SameParameterValue")
  private static boolean startAndCrash(final Random random, final boolean addIndex,
      final boolean addBinaryRecords,
      final boolean useSmallDiskCache, final boolean useSmallWal, final boolean generateOom)
      throws IOException, InterruptedException {

    String javaExec = System.getProperty("java.home") + "/bin/java";
    javaExec = (new File(javaExec)).getCanonicalPath();

    final List<String> commands = new ArrayList<>();

    commands.add(javaExec);
    commands.add("-Xmx2048m");

    if (useSmallDiskCache) {
      commands.add("-Dstorage.diskCache.bufferSize=4096");
    }

    if (useSmallWal) {
      commands.add("-Dstorage.wal.maxSize=4096");
    }

    commands.add("-classpath");
    commands.add(System.getProperty("java.class.path"));

    commands.add(DataLoader.class.getName());

    if (addIndex) {
      commands.add(DataLoader.ADD_INDEX_FLAG);
    }

    if (addBinaryRecords) {
      commands.add(DataLoader.ADD_BINARY_RECORDS_FLAG);
    }

    final ProcessBuilder processBuilder = new ProcessBuilder(commands);

    processBuilder.inheritIO();
    Process process = processBuilder.start();

    final long secondsToWait = random.nextInt(4 * 60 * 60/*4 hours in seconds*/ - 15) + 15;

    logger.info("DataLoader process is started with parameters (addIndex {}, addBinaryRecords {}, "
            + "useSmallDiskCache {}, useSmallWal {}, generate OOM {}), waiting for completion during {} seconds...",
        addIndex,
        addBinaryRecords, useSmallDiskCache, useSmallWal, generateOom, secondsToWait);

    final Timer timer = new Timer();
    timer.schedule(new CrashCountDownTask(secondsToWait, generateOom), 30 * 1000, 30 * 1000);

    final boolean completed = process.waitFor(secondsToWait, TimeUnit.SECONDS);
    timer.cancel();

    if (completed) {
      logger.error("Data load is completed successfully nothing to check");
      return false;
    } else {

      final boolean killSignal = random.nextBoolean();

      if (killSignal) {
        logger.info("Process will be destroyed by sending of KILL signal");
        process.destroyForcibly().waitFor();
      } else {
        logger.info("Process will be destroyed by halting JVM");
        triggerJVMHalt();

        final boolean terminated = process.waitFor(60, TimeUnit.SECONDS);

        if (!terminated) {
          logger.info(
              "Process was not terminated by halting JVM, destroying process by sending of KILL signal");
          process.destroyForcibly().waitFor();
        }
      }

      logger.info("Process is destroyed data integrity check is started");
      return true;
    }
  }

  private static void triggerJVMHalt() throws IOException {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress(InetAddress.getLocalHost(), 2048));
    socket.setSoTimeout(2000);
    OutputStream stream = socket.getOutputStream();
    stream.write(42);
    stream.flush();
    stream.close();
    socket.close();
  }

  private static void checkDatabase(final boolean addIndex, final boolean addBinaryRecords) {
    try (OrientDB orientDB = new OrientDB(DATABASES_URL, OrientDBConfig.defaultConfig())) {
      runDbCheck(DB_NAME, addIndex, addBinaryRecords, orientDB);

      logger.info("DB check is completed, removing DB");
      orientDB.drop(DB_NAME);
    }
  }

  private static long calculateDirectorySize(String path) throws IOException {
    final Path folder = Paths.get(path);

    try (var files = Files.walk(folder)) {
      return files.filter(p -> p.toFile().isFile() && !p.getFileName().toString().endsWith(".wal"))
          .mapToLong(p -> p.toFile().length()).sum();
    }
  }

  private static void runDbCheck(final String dbName, boolean addIndex, boolean addBinaryRecords,
      OrientDB orientDB) {
    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      AtomicInteger counter = new AtomicInteger();

      logger.info("Start DB check");
      try (OResultSet resultSet = session.query("select from " + CRASH_V)) {
        resultSet.vertexStream().forEach(v -> {
          final List<Long> ringIds = v.getProperty(RING_IDS);
          if (ringIds != null) {
            for (Long ringId : ringIds) {
              checkRing(session, v, ringId, addIndex, addBinaryRecords);
            }
          }

          final int cnt = counter.incrementAndGet();
          if (cnt > 0 && cnt % 1000 == 0) {
            logger.info("{} vertexes were checked", cnt);
          }
        });
      }
    }
  }

  private static void checkRing(final ODatabaseSession session, OVertex start, long ringId,
      final boolean addIndex,
      final boolean addBinaryRecords) {
    int vCount = 1;
    OVertex v = start;

    final List<Long> rIds = start.getProperty(RING_IDS);
    final int idIndex = rIds.indexOf(ringId);
    final List<Integer> ringSizes = start.getProperty(RING_SIZES);
    final int ringSize = ringSizes.get(idIndex);

    do {
      for (OEdge e : v.getEdges(ODirection.OUT, CRASH_E)) {
        if (e.getProperty(RING_ID).equals(ringId)) {
          v = e.getTo();

          if (!v.equals(start)) {
            vCount++;

            final List<Long> ringIds = v.getProperty(RING_IDS);
            final int ringIdIndex = ringIds.indexOf(ringId);
            final List<Integer> rSizes = v.getProperty(RING_SIZES);
            final int rSize = rSizes.get(ringIdIndex);

            if (rSize != ringSize) {
              throw new IllegalStateException("Expected and actual ring sizes are not equal");
            }
          }
          break;
        }

        if (addIndex) {
          final int randomValue = e.getProperty(DataLoader.RANDOM_VALUE_FIELD);

          try (final OResultSet resultSet = session
              .query("select * from " + CRASH_E + " where " + RANDOM_VALUE_FIELD + " = "
                  + randomValue)) {
            if (resultSet.edgeStream()
                .noneMatch(edge -> edge.getIdentity().equals(e.getIdentity()))) {
              throw new IllegalStateException(
                  "Random value present inside of edge is absent in index");
            }
          }

          final List<Integer> randomValues = e.getProperty(DataLoader.RANDOM_VALUES_FIELD);

          for (int rndVal : randomValues) {
            try (final OResultSet resultSet = session
                .query("select * from " + CRASH_E + " where " + RANDOM_VALUES_FIELD + " = "
                    + rndVal)) {
              if (resultSet.edgeStream()
                  .noneMatch(edge -> edge.getIdentity().equals(e.getIdentity()))) {
                throw new IllegalStateException(
                    "Random values present inside of edge is absent in index");
              }
            }
          }
        }

        if (addBinaryRecords) {
          final byte[] binaryRecord = e.getProperty(DataLoader.BINARY_FIELD);

          if (binaryRecord == null) {
            throw new IllegalStateException("Binary record is absent");
          }

          final int binaryRecordLength = e.getProperty(DataLoader.BINARY_FIELD_SIZE);

          if (binaryRecord.length != binaryRecordLength) {
            throw new IllegalStateException(
                "Length of binary record does not equal to the stored length");
          }
        }

      }

      if (vCount > ringSize) {
        throw new IllegalStateException("Invalid ring size value");
      }
    } while (!v.equals(start));

    if (vCount != ringSize) {
      throw new IllegalStateException("Expected and actual ring sizes are not equal");
    }
  }
}

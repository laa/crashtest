package com.orientechnologies.crashtest;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class Loader implements Callable<Void> {
  private static final Logger logger = LogManager.getFormatterLogger(Loader.class);

  private static final int MAX_RETRIES = 100_000;
  private final ODatabasePool pool;
  private final AtomicLong    idGen;

  private final boolean addIndex;

  private final boolean addBinaryRecrods;

  private final AtomicBoolean stopFlag;

  private List<byte[]> payLoad = new ArrayList<>();

  Loader(ODatabasePool pool, AtomicLong idGen, boolean addIndex, boolean addBinaryRecords, AtomicBoolean stopFlag) {
    this.pool = pool;
    this.idGen = idGen;
    this.addIndex = addIndex;
    this.addBinaryRecrods = addBinaryRecords;
    this.stopFlag = stopFlag;
  }

  @Override
  public Void call() {
    try {
      final ThreadLocalRandom random = ThreadLocalRandom.current();
      int ringsCounter = 0;

      while (!stopFlag.get()) {
        try (ODatabaseSession session = pool.acquire()) {
          final int ringSize = random.nextInt(10) + 3;

          try {
            session.begin();

            final long ringId = idGen.getAndIncrement();

            OVertex prevVertex = null;
            OVertex firstVertex = null;

            final Set<Integer> vertexIds = new HashSet<>();

            while (vertexIds.size() < ringSize) {
              final int vertexId = random.nextInt(DataLoader.VERTEX_COUNT);
              vertexIds.add(vertexId);
            }

            final List<OVertex> vertices = fetchVertices(session, vertexIds);

            for (int i = 0; i < vertices.size(); i++) {
              OVertex vertex = vertices.get(i);
              List<Long> ringIds = vertex.getProperty(DataLoader.RING_IDS);

              int retryCounter = 0;

              while (ringIds != null && ringIds.size() >= 60) {
                retryCounter++;

                if (retryCounter > MAX_RETRIES) {
                  logger.info("%s - Limit of ring retries is reached, thread stopped, %d rings were created",
                      Thread.currentThread().getName(), ringsCounter);
                  return null;
                }

                vertex = chooseRandomVertex(session, random);
                while (!vertexIds.add(vertex.<Integer>getProperty(DataLoader.V_ID))) {
                  retryCounter++;

                  if (retryCounter > MAX_RETRIES) {
                    logger.info("%s - Limit of ring retries is reached, thread stopped, %d rings were created",
                        Thread.currentThread().getName(), ringsCounter);
                    return null;
                  }

                  vertex = chooseRandomVertex(session, random);
                }

                ringIds = vertex.getProperty(DataLoader.RING_IDS);
                vertices.set(i, vertex);
              }

              if (ringIds == null) {
                ringIds = new ArrayList<>();
                vertex.setProperty(DataLoader.RING_IDS, ringIds);
              }

              ringIds.add(ringId);

              List<Integer> ringSizes = vertex.getProperty(DataLoader.RING_SIZES);
              if (ringSizes == null) {
                ringSizes = new ArrayList<>();
                vertex.setProperty(DataLoader.RING_SIZES, ringSizes);
              }

              ringSizes.add(ringSize);

              if (prevVertex != null) {
                final OEdge edge = prevVertex.addEdge(vertex, DataLoader.CRASH_E);
                edge.setProperty(DataLoader.RING_ID, ringId);

                if (addIndex) {
                  addRandomValues(random, edge);
                }

                if (addBinaryRecrods) {
                  addBinaryRecord(random, edge);
                }

                prevVertex.save();
                edge.save();
              }

              vertex.save();
              prevVertex = vertex;

              if (firstVertex == null) {
                firstVertex = vertex;
              }
            }

            if (prevVertex != null) {
              OEdge edge = prevVertex.addEdge(firstVertex, DataLoader.CRASH_E);
              edge.setProperty(DataLoader.RING_ID, ringId);

              if (addIndex) {
                addRandomValues(random, edge);
              }

              if (addBinaryRecrods) {
                addBinaryRecord(random, edge);
              }

              edge.save();
              prevVertex.save();
              firstVertex.save();
            }

            if (DataLoader.generateOOM.get()) {
              addChunkToHeap(random);
            }

            session.commit();
            ringsCounter++;

            if (ringsCounter > 0 && ringsCounter % 1000 == 0) {
              logger.info("%s thread, %d rings were created", Thread.currentThread().getName(), ringsCounter);
            }
          } catch (ONeedRetryException e) {
            //continue;
          } catch (OutOfMemoryError e) {
            payLoad = null;
            System.gc();
            payLoad = new ArrayList<>();

            logger.error("OOM in loader thread, ignore and repeat");
          }
        }
      }
    } catch (RuntimeException | Error e) {
      logger.error("Error during data load", e);
    }

    if (stopFlag.get()) {
      logger.info("Thread %s was stopped, by stop file", Thread.currentThread().getName());
    }
    return null;
  }

  private void addRandomValues(ThreadLocalRandom random, OEdge edge) {
    final int randomValue = random.nextInt(DataLoader.VERTEX_COUNT / 1000);
    edge.setProperty(DataLoader.RANDOM_VALUE_FIELD, randomValue);

    final int randomValuesSize = random.nextInt(20) + 10;
    final List<Integer> randomValues = new ArrayList<>();

    for (int n = 0; n < randomValuesSize; n++) {
      randomValues.add(random.nextInt(DataLoader.VERTEX_COUNT / 1000));
    }

    edge.setProperty(DataLoader.RANDOM_VALUES_FIELD, randomValues);
  }

  private void addBinaryRecord(ThreadLocalRandom random, OEdge edge) {
    final int binarySize = random.nextInt(25 * 1024) + 1024;
    final byte[] binary = new byte[binarySize];
    random.nextBytes(binary);

    edge.setProperty(DataLoader.BINARY_FIELD, binary);
    edge.setProperty(DataLoader.BINARY_FIELD_SIZE, binarySize);
  }

  private List<OVertex> fetchVertices(ODatabaseSession session, Collection<Integer> vertexIds) {
    final List<OVertex> vertices = new ArrayList<>();

    for (Integer vId : vertexIds) {
      try (OResultSet resultSet = session.query("select from " + DataLoader.CRASH_V + " where " + DataLoader.V_ID + " = ?", vId)) {
        final OResult result = resultSet.next();

        assert result.getVertex().isPresent();
        vertices.add(result.getVertex().get());
      }
    }

    return vertices;
  }

  private OVertex chooseRandomVertex(ODatabaseSession session, ThreadLocalRandom random) {
    final int id = random.nextInt(DataLoader.VERTEX_COUNT);

    try (OResultSet result = session.query("select from " + DataLoader.CRASH_V + " where " + DataLoader.V_ID + " = " + id)) {
      //noinspection ConstantConditions
      return result.next().getVertex().get();
    }
  }

  private void addChunkToHeap(ThreadLocalRandom random) {
    final int chunkSize = random.nextInt(5 * 1024*1024) + 5 * 1024*1024;
    final byte[] chunk = new byte[chunkSize];
    payLoad.add(chunk);
  }

}

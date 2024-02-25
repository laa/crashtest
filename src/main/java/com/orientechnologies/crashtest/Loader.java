package com.orientechnologies.crashtest;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class Loader implements Callable<Void> {
  private static final Logger logger = LogManager.getLogger(Loader.class);

  private static final int           MAX_RETRIES = 100_000;
  private final        ODatabasePool pool;
  private final        AtomicLong    idGen;

  private final boolean addIndex;

  private final boolean addBinaryRecrods;

  private final AtomicBoolean stopFlag;

  private final int vertexesCount;

  private final int iteration;

  private List<byte[]> payLoad = new ArrayList<>();

  Loader(ODatabasePool pool, AtomicLong idGen, boolean addIndex, boolean addBinaryRecords, AtomicBoolean stopFlag,
      int vertexesCount, int iteration) {
    this.pool = pool;
    this.idGen = idGen;
    this.addIndex = addIndex;
    this.addBinaryRecrods = addBinaryRecords;
    this.stopFlag = stopFlag;
    this.vertexesCount = vertexesCount;
    this.iteration = iteration;
  }

  @Override
  public Void call() {
    try {
      final ThreadLocalRandom random = ThreadLocalRandom.current();
      int ringsCreationCounter = 0;
      int ringsDeletionCounter = 0;

      while (!stopFlag.get()) {
        try {
          if (((ringsCreationCounter - ringsDeletionCounter) < vertexesCount) && random.nextDouble() > 0.1
              || ((ringsCreationCounter - ringsDeletionCounter) >= vertexesCount) && random.nextDouble() > 0.5) {
            final int ringsCount = addRing(random);

            if (ringsCount == -1) {
              return null;
            }

            ringsCreationCounter += ringsCount;
          } else {
            ringsDeletionCounter += removeRing(random);
          }
        } catch (OutOfMemoryError e) {
          payLoad = null;
          System.gc();
          payLoad = new ArrayList<>();

          logger.error("OOM in loader thread, ignore and repeat. Iteration {}", iteration);
        }
      }
    } catch (RuntimeException | Error e) {
      logger.error("Error during data load. Iteration " + iteration, e);
      throw e;
    }

    if (stopFlag.get()) {
      logger.info("Thread {} was stopped, by stop file. Iteration {}",
          Thread.currentThread().getName(), iteration);
    }

    return null;
  }

  private int removeRing(ThreadLocalRandom random) {
    try (ODatabaseSession session = pool.acquire()) {
      final int vertexId = random.nextInt(vertexesCount);
      final OVertex ringsStart;

      try (OResultSet resultSet = session
          .query("select from " + DataLoader.CRASH_V + " where " + DataLoader.V_ID + " = ?", vertexId)) {
        final OResult result = resultSet.next();

        assert result.getVertex().isPresent();
        ringsStart = result.getVertex().get();
      }

      List<Long> ringIds = ringsStart.getProperty(DataLoader.RING_IDS);
      if (ringIds == null || ringIds.isEmpty()) {
        return 0;
      }

      session.begin();
      int ringIndex = random.nextInt(ringIds.size());

      final long ringId = ringIds.get(ringIndex);
      final List<OEdge> edgesToDelete = new ArrayList<>();

      ringIds.remove(ringIndex);
      List<Integer> ringSizes = ringsStart.getProperty(DataLoader.RING_SIZES);
      ringSizes.remove(ringIndex);

      ringsStart.save();

      OVertex nextVertex = ringsStart;
      do {
        OVertex prevVertex = nextVertex;

        for (OEdge edge : nextVertex.getEdges(ODirection.OUT, DataLoader.CRASH_E)) {
          if (edge.getProperty(DataLoader.RING_ID).equals(ringId)) {
            edgesToDelete.add(edge);
            nextVertex = edge.getTo();
            break;
          }
        }

        if (prevVertex.getIdentity().equals(nextVertex.getIdentity())) {
          //throw exception once serializable isolation level will be implemented
          session.rollback();
          return 0;
        }

        if (!nextVertex.getIdentity().equals(ringsStart.getIdentity())) {
          ringIds = nextVertex.getProperty(DataLoader.RING_IDS);

          ringIndex = ringIds.indexOf(ringId);
          if (ringIndex < 0) {
            //throw exception once serializable isolation level will be implemented
            session.rollback();
            return 0;
          }

          ringIds.remove(ringIndex);

          ringSizes = nextVertex.getProperty(DataLoader.RING_SIZES);
          ringSizes.remove(ringIndex);

          nextVertex.save();
        }

      } while (!nextVertex.getIdentity().equals(ringsStart.getIdentity()));

      for (OEdge edge : edgesToDelete) {
        edge.delete();
      }

      session.commit();

    } catch (ONeedRetryException e) {
      return 0;
    }

    return 1;
  }

  private int addRing(ThreadLocalRandom random) {
    int ringsCounter = 0;

    try (ODatabaseSession session = pool.acquire()) {
      final int ringSize = random.nextInt(10) + 3;

      try {
        session.begin();

        final long ringId = idGen.getAndIncrement();

        OVertex prevVertex = null;
        OVertex firstVertex = null;

        final Set<Integer> vertexIds = new HashSet<>();

        while (vertexIds.size() < ringSize) {
          final int vertexId = random.nextInt(vertexesCount);
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
              logger.info("{}- Limit of ring retries is reached, thread stopped, "
                      + "{} rings were created. Iteration {}",
                  Thread.currentThread().getName(), ringsCounter, iteration);
              return -1;
            }

            vertex = chooseRandomVertex(session, random);
            while (!vertexIds.add(vertex.<Integer>getProperty(DataLoader.V_ID))) {
              retryCounter++;

              if (retryCounter > MAX_RETRIES) {
                logger.info("{} - Limit of ring retries is reached,"
                        + " thread stopped, {} rings were created. Iteration {}",
                    Thread.currentThread().getName(), ringsCounter, iteration);
                return -1;
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

      } catch (ONeedRetryException e) {
        //continue;
      }
    }

    return ringsCounter;
  }

  private void addRandomValues(ThreadLocalRandom random, OEdge edge) {
    final int randomValue = random.nextInt(vertexesCount / 1000);
    edge.setProperty(DataLoader.RANDOM_VALUE_FIELD, randomValue);

    final int randomValuesSize = random.nextInt(20) + 10;
    final List<Integer> randomValues = new ArrayList<>();

    for (int n = 0; n < randomValuesSize; n++) {
      randomValues.add(random.nextInt(vertexesCount / 1000));
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
    final int id = random.nextInt(vertexesCount);

    try (OResultSet result = session.query("select from " + DataLoader.CRASH_V + " where " + DataLoader.V_ID + " = " + id)) {
      //noinspection OptionalGetWithoutIsPresent
      return result.next().getVertex().get();
    }
  }

  private void addChunkToHeap(ThreadLocalRandom random) {
    final int chunkSize = random.nextInt(5 * 1024 * 1024) + 5 * 1024 * 1024;
    final byte[] chunk = new byte[chunkSize];
    payLoad.add(chunk);
  }

}

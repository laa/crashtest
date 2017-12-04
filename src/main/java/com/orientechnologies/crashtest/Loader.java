package com.orientechnologies.crashtest;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class Loader implements Callable<Void> {
  private static final Logger logger = LogManager.getFormatterLogger(Loader.class);

  private static final int MAX_RETRIES = 100_000;
  private final ODatabasePool pool;
  private final AtomicLong    idGen;

  Loader(ODatabasePool pool, AtomicLong idGen) {
    this.pool = pool;
    this.idGen = idGen;
  }

  @Override
  public Void call() throws Exception {
    try {
      final ThreadLocalRandom random = ThreadLocalRandom.current();
      int ringsCounter = 0;

      while (true) {
        try (ODatabaseSession session = pool.acquire()) {
          final int ringSize = random.nextInt(10) + 3;
          try {
            session.begin();

            final Set<OVertex> ringVertices = new HashSet<>();
            final long ringId = idGen.getAndIncrement();

            OVertex prevVertex = null;
            OVertex firstVertex = null;

            while (ringVertices.size() < ringSize) {
              OVertex v = chooseRandomVertex(session, random);
              List<Long> ringIds = v.getProperty(DataLoader.RING_IDS);

              int retryCounter = 0;
              do {
                if (ringVertices.contains(v)) {
                  retryCounter++;

                  if (retryCounter > MAX_RETRIES) {
                    logger.info("%s - Limit of ring retries is reached, thread stopped, %d rings were created",
                        Thread.currentThread().getName(), ringsCounter);
                    return null;
                  }

                  v = chooseRandomVertex(session, random);
                  ringIds = v.getProperty(DataLoader.RING_IDS);
                } else {
                  while (ringIds != null && ringIds.size() >= 60) {
                    retryCounter++;

                    if (retryCounter > MAX_RETRIES) {
                      logger.info("%s - Limit of ring retries is reached, thread stopped, %d rings were created",
                          Thread.currentThread().getName(), ringsCounter);
                      return null;
                    }

                    v = chooseRandomVertex(session, random);
                    ringIds = v.getProperty(DataLoader.RING_IDS);
                  }
                }
              } while (!ringVertices.add(v));

              if (ringIds == null) {
                ringIds = new ArrayList<>();
                v.setProperty(DataLoader.RING_IDS, ringIds);
              }

              ringIds.add(ringId);

              List<Integer> ringSizes = v.getProperty(DataLoader.RING_SIZES);
              if (ringSizes == null) {
                ringSizes = new ArrayList<>();
                v.setProperty(DataLoader.RING_SIZES, ringSizes);
              }

              ringSizes.add(ringSize);

              if (prevVertex != null) {
                final OEdge edge = prevVertex.addEdge(v, DataLoader.CRASH_E);
                edge.setProperty(DataLoader.RING_ID, ringId);

                prevVertex.save();
                edge.save();
              }

              v.save();
              prevVertex = v;

              if (firstVertex == null) {
                firstVertex = v;
              }
            }

            if (prevVertex != null) {
              OEdge edge = prevVertex.addEdge(firstVertex, DataLoader.CRASH_E);
              edge.setProperty(DataLoader.RING_ID, ringId);

              edge.save();
              prevVertex.save();
              firstVertex.save();
            }

            session.commit();
            ringsCounter++;

            if (ringsCounter > 0 && ringsCounter % 1000 == 0) {
              logger.info("%s thread, %d rings were created", Thread.currentThread().getName(), ringsCounter);
            }
          } catch (ONeedRetryException e) {
            //continue;
          }
        }
      }
    } catch (RuntimeException | Error e) {
      logger.error("Error during data load", e);
      throw e;
    }
  }

  private OVertex chooseRandomVertex(ODatabaseSession session, ThreadLocalRandom random) {
    final int id = random.nextInt(DataLoader.VERTEX_COUNT);

    try (OResultSet result = session.query("select from " + DataLoader.CRASH_V + " where " + DataLoader.V_ID + " = " + id)) {
      //noinspection ConstantConditions
      return result.next().getVertex().get();
    }
  }
}

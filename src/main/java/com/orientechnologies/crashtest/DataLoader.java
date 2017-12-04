package com.orientechnologies.crashtest;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class DataLoader {
  private static final Logger logger = LogManager.getFormatterLogger(DataLoader.class);

  static final int    VERTEX_COUNT  = 10_000_000;
  static final String V_ID          = "id";
  static final String RING_IDS      = "ringIds";
  static final String RING_SIZES    = "ringSizes";
  static final String RING_ID       = "ringId";
  static final String CRASH_V       = "CrashV";
  static final String CRASH_E       = "CrashE";
  static final String DB_NAME       = "crashdb";
  static final String DATABASES_DIR = "plocal:target/databases";

  public static void main(String[] args) throws Exception {
    OrientDB orientDB = new OrientDB(DATABASES_DIR, OrientDBConfig.defaultConfig());
    if (orientDB.exists("crashdb")) {
      orientDB.drop("crashdb");
    }

    orientDB.create(DB_NAME, ODatabaseType.PLOCAL);

    final ODatabaseSession session = orientDB.open("crashdb", "admin", "admin");
    final OClass vCls = session.createVertexClass(CRASH_V);

    vCls.createProperty(V_ID, OType.INTEGER).createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
    vCls.createProperty(RING_IDS, OType.EMBEDDEDLIST);
    vCls.createProperty(RING_SIZES, OType.EMBEDDEDLIST);

    final OClass eCls = session.createEdgeClass(CRASH_E);
    eCls.createProperty(RING_ID, OType.LONG);

    logger.info("Start vertex addition");
    for (int i = 0; i < VERTEX_COUNT; i++) {
      OVertex vertex = session.newVertex(vCls);
      vertex.setProperty("id", i);
      vertex.save();

      if (i > 0 && i % 100_000 == 0) {
        logger.info("%d vertexes were added", i);
      }
    }

    session.close();

    final ExecutorService loaderService = Executors.newCachedThreadPool();
    final List<Future<Void>> futures = new ArrayList<>();
    final ODatabasePool pool = new ODatabasePool(orientDB, "crashdb", "admin", "admin");
    final AtomicLong idGen = new AtomicLong();

    logger.info("%d vertexes were created", VERTEX_COUNT);
    logger.info("Start rings creation");
    for (int i = 0; i < 8; i++) {
      futures.add(loaderService.submit(new Loader(pool, idGen)));
    }

    for (Future<Void> future : futures) {
      future.get();
    }

    pool.close();
    orientDB.close();
  }
}

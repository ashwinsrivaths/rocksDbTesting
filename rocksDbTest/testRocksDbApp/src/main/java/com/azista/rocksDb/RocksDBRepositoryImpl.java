package com.azista.rocksDb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBRepositoryImpl implements KeyValueRepository<String, String> {
      private final static String NAME = "first-db";
  File dbDir;
  RocksDB db;



    void initialize() {
    RocksDB.loadLibrary();
    final Options options = new Options();
    options.setCreateIfMissing(true);
    dbDir = new File("/tmp/rocks-db", NAME);
    try {
        
       System.out.println( dbDir.toPath());
    //   System.out.println(dbDir.getAbsoluteFile().toPath());


      Files.createDirectories(dbDir.getParentFile().toPath());
      Files.createDirectories(dbDir.getAbsoluteFile().toPath());

      db = RocksDB.open(options, dbDir.getAbsolutePath());
    } catch(IOException | RocksDBException ex) {
      System.out.println("Error initializng RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}");
    }
    System.out.println("RocksDB initialized and ready to use");
  }

  @Override
  public synchronized void save(String key, String value) {
    System.out.println("save");
    try {
      db.put(key.getBytes(), value.getBytes());
    } catch (RocksDBException e) {
      System.out.println("Error saving entry in RocksDB, cause: {}, message: {}");
    }
  }


  @Override
  public String find(String key) {
    System.out.println("find");
    String result = null;
    try {
      byte[] bytes = db.get(key.getBytes());
      if(bytes == null) return null;
      result = new String(bytes);
    } catch (RocksDBException e) {
            System.out.println("Error retrieving the entry in RocksDB from key: {}, cause: {}, message: {}");
    }
    return result;
  }

  @Override
  public void delete(String key) {
    System.out.println("delete");
    try {
      db.delete(key.getBytes());
    } catch (RocksDBException e) {
            System.out.println("Error deleting entry in RocksDB, cause: {}, message: {}");
    }
  }
}
  



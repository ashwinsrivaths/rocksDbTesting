package com.azista.rocksDb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Hello world!
 *
 */
public class App {

    RocksDBRepositoryImpl db;

    public App() {

        db = new RocksDBRepositoryImpl();
        db.initialize();

    }

    public static void main(String[] args) {
        System.out.println("Hello World!////////////////////////");

        App a = new App();

        a.db.save("null1234", "null431431134gsg");

        System.out.println("........");
        System.out.println(a.db.find("null1234"));
        System.out.println(a.db.find("null12345"));
    }

}

package com.hll.dist.scheduler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by huangll on 2016/12/25.
 */
public class WorkerServer {

  private static ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  public static void main(String[] args) throws IOException {
    ServerSocket ssc = new ServerSocket(9999);
    System.out.println("worker server started...");
    while (true){
      Socket socket = ssc.accept();
      threadPool.execute( new WorkerRunnable(socket));
    }
  }
}

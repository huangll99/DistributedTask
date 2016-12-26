package com.hll.dist.scheduler;

import com.hll.dist.common.Constants;

import java.io.*;
import java.net.Socket;

/**
 * Created by huangll on 2016/12/25.
 */
public class WorkerRunnable implements Runnable {
  private Socket socket;
  private InputStream in;
  private int jarSize;
  private int confSize;

  public WorkerRunnable(Socket socket) {
    this.socket = socket;
  }

  @Override
  public void run() {
    try {
      in = socket.getInputStream();
      byte[] protocal = new byte[7];
      int read = in.read(protocal, 0, 7);
      if (read < 7) {
        System.out.println("protocal error...");
        return;
      }
      String command = new String(protocal);
      switch (command) {
        case "jarfile":
          receiveJarFile();
          break;
        case "jobconf":
          receiveConfFile();
          break;
        case "job2run":
          runJob();
          break;
        default:
          System.out.println("cannot find command...");
          socket.close();
          break;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void runJob() throws IOException, InterruptedException {
    byte[] bytes = new byte[4096];
    int read = in.read(bytes);
    String command = new String(bytes, 0, read);
    System.out.println("receive command:" + command);
    in.close();
    socket.close();

//    File jarFile = new File(Constants.TASK_WORK_DIR + File.separator + Constants.JAR_PATH);
//    File confFile = new File(Constants.TASK_WORK_DIR + File.separator + Constants.CONF_FILE);

    System.out.println("start process data...");
    Process process = Runtime.getRuntime().exec(command);
    int waitFor = process.waitFor();

    if (waitFor == 0) {
      System.out.println("task run success...");
    }else {
      System.out.println("task run fail...");
    }
  }

  private void receiveConfFile() throws IOException {
    System.out.println("receive conf file...");
    FileOutputStream fos = new FileOutputStream(Constants.TASK_WORK_DIR + File.separator + Constants.CONF_FILE);
    byte[] bytes = new byte[4096];
    int read;
    while ((read = in.read(bytes)) != -1) {
      confSize += read;
      fos.write(bytes, 0, read);
    }
    fos.flush();
    fos.close();
    in.close();
    socket.close();
  }

  private void receiveJarFile() throws IOException {
    System.out.println("receive jar file...");
    FileOutputStream fos = new FileOutputStream(Constants.TASK_WORK_DIR + File.separator + "job.jar");
    byte[] bytes = new byte[4096];
    int read;
    while ((read = in.read(bytes)) != -1) {
      jarSize += read;
      fos.write(bytes, 0, read);
    }
    fos.flush();
    fos.close();
    in.close();
    socket.close();
  }
}

package com.hll.dist.scheduler;

import com.hll.dist.common.Constants;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * Created by huangll on 2016/12/25.
 */
public class WorkerClient {

  private HashMap<String, String> conf;
  private Socket socket;
  private OutputStream out;

  public WorkerClient(HashMap<String, String> conf) {
    this.conf = conf;
  }

  public void submit() throws IOException {
    socket = new Socket(conf.get(Constants.WORKER_HOST), Integer.parseInt(conf.get(Constants.WORKER_PORT)));
    out = socket.getOutputStream();

    //发送jar包
    String jarPath = conf.get(Constants.JAR_PATH);
    byte[] bytes = new byte[4096];
    FileInputStream jarFIS = new FileInputStream(jarPath);
    out.write("jarfile".getBytes(StandardCharsets.UTF_8));
    int read;
    while ((read = jarFIS.read(bytes)) != -1) {
      out.write(bytes, 0, read);
    }
    jarFIS.close();
    out.close();
    socket.close();

    //发送job.conf文件
    socket = new Socket(conf.get(Constants.WORKER_HOST), Integer.parseInt(conf.get(Constants.WORKER_PORT)));
    out = socket.getOutputStream();
    FileInputStream confFIS = new FileInputStream(Constants.CONF_FILE);
    out.write("jobconf".getBytes(StandardCharsets.UTF_8));
    while ((read = confFIS.read(bytes)) != -1) {
      out.write(bytes, 0, read);
    }
    confFIS.close();
    out.close();
    socket.close();

    //发送启动命令
    socket = new Socket(conf.get(Constants.WORKER_HOST), Integer.parseInt(conf.get(Constants.WORKER_PORT)));
    out = socket.getOutputStream();
    out.write("job2run".getBytes(StandardCharsets.UTF_8));
    String command = "java -cp c:/taskworkdir/job.jar com.hll.dist.task.TaskProcessor";
    out.write(command.getBytes(StandardCharsets.UTF_8));
    out.close();
    socket.close();

  }
}

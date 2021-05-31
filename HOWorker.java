import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.data.Stat;

class HOWorker implements Watcher, StatCallback {
    public ZooKeeper zk;
    public int id;
    public int numWorker;
    public List<Float> grads;
    public int curr_iter;

    public HOWorker(String addrs, int id, int numWorker) throws KeeperException, IOException {
        this.zk = new ZooKeeper(addrs, 3000, this);
        this.id = id;
        this.numWorker = numWorker;
        this.grads = null;
        this.curr_iter = 0;
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        if (args.length < 5) {
            System.err.println("USAGE: HOWorker workerNumber numWorker numEpochs dataFile [host:port ...]");
            System.exit(2);
        }

        int workerId = Integer.parseInt(args[0]);
        int numEpochs = Integer.parseInt(args[2]);
        int numWorker = Integer.parseInt(args[1]);
        String addrs = args[4];
        for (int i = 4; i < args.length; i++)
            addrs += "," + args[i];
        HOWorker worker = new HOWorker(addrs, workerId, numWorker);
        
        if (worker.zk.exists("/w" + workerId, false) == null) {
            worker.zk.create("/w" + workerId, null, null, CreateMode.PERSISTENT);
        }

        for (int i = 0; i < numWorker; i++) {
            while (worker.zk.exists("/w" + i, false) == null);
        }
        
        for(int k = 0; k < numEpochs; k++) {
            worker.curr_iter = k;
            int previousWorkerIdToListen = worker.getWorkerIdToListen();
            worker.zk.exists("/w" + previousWorkerIdToListen, true, worker, null);

            ProcessBuilder pb = new ProcessBuilder("python", "compute_gradient.py", args[3]);
            Process process = pb.start();
            if (process.waitFor() != 0)
                return;

            List<Float> grads = new ArrayList<Float>();
            try {
                File file = new File("grads.txt");
                Scanner scanner = new Scanner(file);
                while (scanner.hasNextFloat()) {
                    grads.add(scanner.nextFloat());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            worker.grads = grads;
            worker.writeGradToZnode(0);
            for (int i = 0; i < numWorker; i++) {
                while (worker.zk.exists("/start" + i + "w" + k, false) == null);
            }
        }
    }

    private List<Float> convertByteToFloat(byte[] data) {
        List<Float> list = new ArrayList<>();
        for (int j = 0; j < data.length / 4; j++) {
            list.add(ByteBuffer.wrap(data, 4 * j, 4).getFloat());
        }
        return list;
    }

    private void writeGradToZnode(int statusCode) throws KeeperException, InterruptedException {
        byte[] vector = new byte[this.grads.size() * 4+1];
        for (int i = 0; i < this.grads.size(); i++) {
            byte[] byteRep = ByteBuffer.allocate(4).putFloat(grads.get(i)).array();
            for (int j = 0; j < 4; j++)
                vector[4 * i + j] = byteRep[j];
        }
        byte[] byteRep = ByteBuffer.allocate(4).putInt(statusCode).array();
        for (int j = 4; j > 0; j--)
            vector[vector.length - j] = byteRep[4 - j];
        this.zk.setData("/w" + this.id, vector, -1);
    }

    private int getWorkerIdToListen() {
        int previousWorkerIdToListen;
        if (this.id > 0) {
            previousWorkerIdToListen = this.id - 1;
        }
        else {
            previousWorkerIdToListen = this.numWorker - 1;
        }
        return previousWorkerIdToListen;
    }

    private void writeGradToFile() {
        // This condition means all the data needs to get the updated all the grad.
        try {
            FileWriter fw = new FileWriter(new File("grads.txt"), false);
            for (int j = 0; j < this.grads.size(); j++) {
                fw.write(this.grads.get(j) + "\n");
            }
            fw.close();

            ProcessBuilder pb = new ProcessBuilder("python", "update_params.py");
            Process process = pb.start();
            if (process.waitFor() != 0)
                return;
            this.zk.create("/start" + this.id + "w" + this.curr_iter, null, null, CreateMode.PERSISTENT);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent event) {
        try {
            byte[] data = this.zk.getData(event.getPath(), false, this.zk.exists(event.getPath(), false));

            if (data[data.length - 1] == 0 ) {
                
                if (this.id == 1) {
                    List<Float> previousGrad = this.convertByteToFloat(data);
                    for (int i = 0; i < previousGrad.size(); i++) {
                        this.grads.set(i, this.grads.get(i) + previousGrad.get(i));
                    }
                    this.writeGradToZnode(1);
                }
            }
            else if (data[data.length - 1] == 1) {
                if (this.id == 0) {
                    List<Float> previousGrad = this.convertByteToFloat(data);
                    this.grads = previousGrad;
                    this.writeGradToZnode(2);
                    this.writeGradToFile();
                }
                else {
                    List<Float> previousGrad = this.convertByteToFloat(data);
                    for (int i = 0; i < previousGrad.size(); i++) {
                        this.grads.set(i, this.grads.get(i) + previousGrad.get(i));
                    }
                    this.writeGradToZnode(1);

                    if (this.id == this.numWorker - 1) {
                        this.writeGradToFile();
                    }
                }
            }
            else if(this.id > 0 && this.id < this.numWorker -1 ) {
                List<Float> previousGrad = this.convertByteToFloat(data);
                this.grads = previousGrad;
                this.writeGradToZnode(2);
                this.writeGradToFile();
            }

            //worker.zk.create();
            int previousWorkerIdToListen = this.getWorkerIdToListen();
            this.zk.exists("/w" + previousWorkerIdToListen, true, this, null);
            // this.zk.create("/ack" + this.id, null, null, CreateMode.PERSISTENT);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void processResult(int rc, String path, Object ctx, Stat stat) {
        if (rc != Code.OK.intValue()) {
            System.out.println("Something's wrong: " + path);
        }
    }
}
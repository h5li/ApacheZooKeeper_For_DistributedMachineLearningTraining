import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.ACL;

class HOWorker implements Watcher, StatCallback {
    public ZooKeeper zk;
    public int id;
    public int numWorker;
    public List<Double> grads;
    public int curr_iter;
    public static final List<ACL> OPEN_ACL_UNSAFE = new LinkedList<ACL>();

    public HOWorker(String addrs, int id, int numWorker) throws KeeperException, IOException {
        this.zk = new ZooKeeper(addrs, 3000, this);
        this.id = id;
        this.numWorker = numWorker;
        this.grads = null;
        this.curr_iter = 0;
        OPEN_ACL_UNSAFE.add(new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE));
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
        for (int i = 5; i < args.length; i++)
            addrs += "," + args[i];
        HOWorker worker = new HOWorker(addrs, workerId, numWorker);
        
        if (worker.zk.exists("/w" + workerId, false) == null) {
            worker.zk.create("/w" + workerId, null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        for (int i = 0; i < numWorker; i++) {
            while (worker.zk.exists("/w" + i, false) == null);
        }
        
        for(int k = 0; k < numEpochs; k++) {
            worker.curr_iter = k;
            int previousWorkerIdToListen = worker.getWorkerIdToListen();
            worker.zk.exists("/w" + previousWorkerIdToListen, true, worker, null);

            ProcessBuilder pb = new ProcessBuilder("python", "compute_gradient.py", args[3], ""+workerId);
            Process process = pb.start();
            if (process.waitFor() != 0)
                return;

            List<Double> grads = new ArrayList<Double>();
            try {
                File file = new File("grads"+workerId+".txt");
                Scanner scanner = new Scanner(file);
                while (scanner.hasNextDouble()) {
                    grads.add(scanner.nextDouble());
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

    private List<Double> convertByteToDouble(byte[] data) {
        List<Double> list = new ArrayList<>();
        for (int j = 0; j < data.length / 8; j++) {
            list.add(ByteBuffer.wrap(data, 8 * j, 8).getDouble());
        }
        return list;
    }

    private void writeGradToZnode(int statusCode) throws KeeperException, InterruptedException {
        byte[] vector = new byte[this.grads.size() * 8 + 1];
        for (int i = 0; i < this.grads.size(); i++) {
            byte[] byteRep = ByteBuffer.allocate(8).putDouble(grads.get(i)).array();
            for (int j = 0; j < 8; j++)
                vector[8 * i + j] = byteRep[j];
        }
        byte[] byteRep = ByteBuffer.allocate(8).putInt(statusCode).array();
        for (int j = 8; j > 0; j--)
            vector[vector.length - j] = byteRep[8 - j];
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

            String path = "/start" + this.id + "w" + this.curr_iter;
            if (this.zk.exists(path, false) == null)
                this.zk.create(path, null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent event) {
        if (event.getPath() == null)
            return;

        try {
            byte[] data = this.zk.getData(event.getPath(), false, this.zk.exists(event.getPath(), false));

            if (data[data.length - 1] == 0 ) {
                
                if (this.id == 1) {
                    List<Double> previousGrad = this.convertByteToDouble(data);
                    for (int i = 0; i < previousGrad.size(); i++) {
                        this.grads.set(i, this.grads.get(i) + previousGrad.get(i));
                    }
                    this.writeGradToZnode(1);
                }
            }
            else if (data[data.length - 1] == 1) {
                if (this.id == 0) {
                    List<Double> previousGrad = this.convertByteToDouble(data);
                    this.grads = previousGrad;
                    this.writeGradToZnode(2);
                    this.writeGradToFile();
                }
                else {
                    List<Double> previousGrad = this.convertByteToDouble(data);
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
                List<Double> previousGrad = this.convertByteToDouble(data);
                this.grads = previousGrad;
                this.writeGradToZnode(2);
                this.writeGradToFile();
            }

            // worker.zk.create();
            int previousWorkerIdToListen = this.getWorkerIdToListen();
            this.zk.exists("/w" + previousWorkerIdToListen, true, this, null);
            // worker.zk.create("/ack" + this.id, null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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

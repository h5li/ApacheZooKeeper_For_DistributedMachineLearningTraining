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

class PSWorker implements Watcher, StatCallback {
    public ZooKeeper zk;
    public int id;
    public static final List<ACL> OPEN_ACL_UNSAFE = new LinkedList<ACL>();

    public PSWorker(String addrs, int id) throws KeeperException, IOException {
        this.zk = new ZooKeeper(addrs, 3000, this);
        this.id = id;
        OPEN_ACL_UNSAFE.add(new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE));
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        if (args.length < 4) {
            System.err.println("USAGE: PSWorker workerNumber numEpochs dataFile [host:port ...]");
            System.exit(2);
        }

        int workerId = Integer.parseInt(args[0]);
        int numEpochs = Integer.parseInt(args[1]);
        String addrs = args[3];
        for (int i = 4; i < args.length; i++)
            addrs += "," + args[i];
        PSWorker worker = new PSWorker(addrs, workerId);

        long workerTime = System.nanoTime();
        for(int k = 0; k < numEpochs; k++) {
            long startWait = System.nanoTime();
            while(worker.zk.exists("/start" + k, false) == null && worker.zk.exists("/m", false) == null);
            startWait = System.nanoTime() - startWait;
            System.out.println("Worker " + worker.id + " waited for /start for " + startWait + " ns in iteration " + k);
            worker.zk.exists("/m", true, worker, null);

            ProcessBuilder pb = new ProcessBuilder("python3", "compute_gradient.py", args[2], ""+workerId);
            long pythonExecTime = System.nanoTime();
            Process process = pb.start();
            int status = process.waitFor();
            pythonExecTime = System.nanoTime() - pythonExecTime;
            System.out.println("Worker " + worker.id + " compute_gradient.py ran for " + pythonExecTime + " ns in iteration " + k);
            if (status != 0) {
                System.out.println("Worker " + worker.id + " compute_gradient.py exited abnormally: " + status);
                return;
            }

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

            byte[] vector = new byte[grads.size() * 8];
            for (int i = 0; i < grads.size(); i++) {
                byte[] byteRep = ByteBuffer.allocate(8).putDouble(grads.get(i)).array();
                for (int j = 0; j < 8; j++)
                    vector[8 * i + j] = byteRep[j];
            }
            long setTime = System.nanoTime();
            worker.zk.setData("/w" + workerId, vector, -1);
            setTime = System.nanoTime() - setTime;
            System.out.println("Worker " + worker.id + " set time = " + setTime + " ns in iteration " + k);

            long endWait = System.nanoTime();
            while(worker.zk.exists("/end" + k, false) == null);
            endWait = System.nanoTime() - endWait;
            System.out.println("Worker " + worker.id + " waited for /end for " + endWait + " ns in iteration " + k);
            if (worker.zk.exists("/ack" + worker.id, false) != null)
                worker.zk.delete("/ack" + worker.id, -1);
        }
        workerTime = System.nanoTime() - workerTime;
        System.out.println("Worker " + worker.id + " total time = " + workerTime + " ns");
    }

    public void process(WatchedEvent event) {
        String path = event.getPath();
        if (path == null)
            return;

        try {
            long processTime = System.nanoTime();
            byte[] data = this.zk.getData(path, false, this.zk.exists(path, false));

            FileWriter fw = new FileWriter(new File("grads"+this.id+".txt"), false);
            for (int j = 0; j < data.length / 8; j++) {
                fw.write(ByteBuffer.wrap(data, 8 * j, 8).getDouble() + "\n");
            }
            fw.close();

            ProcessBuilder pb = new ProcessBuilder("python3", "update_params.py", "" + this.id);
            long pythonExecTime = System.nanoTime();
            Process process = pb.start();
            int status = process.waitFor();
            pythonExecTime = System.nanoTime() - pythonExecTime;
            System.out.println("Worker " + this.id + " update_params.py ran for " + pythonExecTime + " ns");
            if (status != 0) {
                System.out.println("Worker " + this.id + " update_params.py exited abnormally: " + status);
                return;
            }
            if (this.zk.exists("/ack" + this.id, false) == null)
                this.zk.create("/ack" + this.id, null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            processTime = System.nanoTime() - processTime;
            System.out.println("Worker " + this.id + " process watch event time = " + processTime + " ns");
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

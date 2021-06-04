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

class Cleaner implements Watcher, StatCallback {
    public ZooKeeper zk;
    public int id;
    public int numWorker;
    public List<Double> grads;
    public int curr_iter;
    public List<Double> previousGrad;
    public static final List<ACL> OPEN_ACL_UNSAFE = new LinkedList<ACL>();

    public Cleaner(String addrs, int id, int numWorker) throws KeeperException, IOException {
        this.zk = new ZooKeeper(addrs, 3000, this);
        this.id = id;
        this.numWorker = numWorker;
        this.grads = null;
        this.curr_iter = 0;
	    this.previousGrad = null;
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
        Cleaner worker = new Cleaner(addrs, workerId, numWorker);

        for (int i = 0; i < numWorker; i++) {
            for (int j = 0; j < numEpochs; j++) {
                if (worker.zk.exists("/start" + i + "w" + j, false) != null)
                    worker.zk.delete("/start" + i + "w" + j, -1);
            }
        }
    }


    public void process(WatchedEvent event) {
	    return;
    }

    public void processResult(int rc, String path, Object ctx, Stat stat) {
        if (rc != Code.OK.intValue()) {
            System.out.println("Something's wrong: " + path);
        }
    }
}

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.data.Stat;

class PSManager implements Watcher, StatCallback {
    public ZooKeeper zk;
    public float[] gradient;
    public int numWorkers;
    public int numDone;

    public PSManager(int numWorkers, String addrs) throws KeeperException, IOException {
        this.gradient = null;
        this.numWorkers = numWorkers;
        this.zk = new ZooKeeper(addrs, 3000, this);
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        if (args.length < 2) {
            System.err.println("USAGE: PSManager numWorkers numEpochs [host:port ...]");
            System.exit(2);
        }

        int numWorkers = Integer.parseInt(args[0]);
        int numEpochs = Integer.parseInt(args[1]);
        String addrs = args[2];
        for (int i = 3; i < args.length; i++)
            addrs += "," + args[i];
        PSManager manager = new PSManager(numWorkers, addrs);

        if (manager.zk.exists("/m", false) == null)
            manager.zk.create("/m", null, null, CreateMode.PERSISTENT);
        for (int i = 0; i < numWorkers; i++) {
            String path = "/w" + i;
            if (manager.zk.exists(path, false) == null)
                manager.zk.create(path, null, null, CreateMode.PERSISTENT);
        }

        for (int k = 0; k < numEpochs; k++) {
            for (int i = 0; i < numWorkers; i++)
                manager.zk.exists("/w" + i, true, manager, null);
            manager.numDone = 0;

            manager.zk.create("/start" + k, null, null, CreateMode.PERSISTENT);
            if (k > 0)
                manager.zk.delete("/end" + (k - 1), -1);
            while (manager.numDone != numWorkers);

            byte[] vector = new byte[manager.gradient.length * 4];
            for (int i = 0; i < manager.gradient.length; i++) {
                byte[] byteRep = ByteBuffer.allocate(4).putFloat(manager.gradient[i]).array();
                for (int j = 0; j < 4; j++)
                    vector[4 * i + j] = byteRep[j];
            }
            manager.zk.setData("/m", vector, -1);
            for (int i = 0; i < numWorkers; i++) {
                while (manager.zk.exists("/ack" + i, false) == null);
            }

            manager.zk.delete("/start" + k, -1);
            manager.zk.create("/end" + k, null, null, CreateMode.PERSISTENT);
        }
    }

    public void process(WatchedEvent event) {
        try {
            byte[] data = this.zk.getData(event.getPath(), false, this.zk.exists(event.getPath(), false));

            if (this.gradient == null)
                this.gradient = new float[data.length / 4];
            if (this.numDone == 0) {
                for (int j = 0; j < this.gradient.length; j++)
                    this.gradient[j] = ByteBuffer.wrap(data, 4 * j, 4).getFloat();
            } else {
                for (int j = 0; j < this.gradient.length; j++)
                    this.gradient[j] += ByteBuffer.wrap(data, 4 * j, 4).getFloat();
            }
            this.numDone++;
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
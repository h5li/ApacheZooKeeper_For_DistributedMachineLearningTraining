import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.ArrayList;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.ACL;

class PSServer implements Watcher, StatCallback {
    public ZooKeeper zk;
    public float[] gradient;
    public int numWorkers;
    public int numDone;
    public static final ArrayList<ACL> OPEN_ACL_UNSAFE = new ArrayList<ACL>(
        Collections.singletonList(new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE)));

    public PSServer(int numWorkers, String addrs) throws KeeperException, IOException {
        this.gradient = new float[785];
        this.numWorkers = numWorkers;
        this.zk = new ZooKeeper(addrs, 3000, this);
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        if (args.length < 3) {
            System.err.println("USAGE: PSServer numWorkers numEpochs [host:port ...]");
            System.exit(2);
        }

        int numWorkers = Integer.parseInt(args[0]);
        int numEpochs = Integer.parseInt(args[1]);
        String addrs = args[2];
        for (int i = 3; i < args.length; i++)
            addrs += "," + args[i];
        PSServer server = new PSServer(numWorkers, addrs);

        if (server.zk.exists("/m", false) == null)
            server.zk.create("/m", null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        for (int i = 0; i < numWorkers; i++) {
            String path = "/w" + i;
            if (server.zk.exists(path, false) == null)
                server.zk.create(path, null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        for (int k = 0; k < numEpochs; k++) {
            for (int i = 0; i < numWorkers; i++)
                server.zk.exists("/w" + i, true, server, null);
            server.numDone = 0;
	    if (server.zk.exists("/start" + k, false) == null)
                server.zk.create("/start" + k, null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            if (k > 0)
                server.zk.delete("/end" + (k - 1), -1);
            while (server.numDone != numWorkers);

            byte[] vector = new byte[server.gradient.length * 4];
            for (int i = 0; i < server.gradient.length; i++) {
                byte[] byteRep = ByteBuffer.allocate(4).putFloat(server.gradient[i]).array();
                for (int j = 0; j < 4; j++)
                    vector[4 * i + j] = byteRep[j];
            }
            server.zk.setData("/m", vector, -1);
            for (int i = 0; i < numWorkers; i++) {
                while (server.zk.exists("/ack" + i, false) == null);
            }

            server.zk.delete("/start" + k, -1);
            server.zk.create("/end" + k, null, null, CreateMode.PERSISTENT);
        }
    }

    public synchronized void process(WatchedEvent event) {
	if (event.getPath() == null) return;
        try {
            byte[] data = this.zk.getData(event.getPath(), false, this.zk.exists(event.getPath(), false));

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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.data.Stat;

class PSWorker implements DataCallback {
    public float[] vector;

    public PSWorker() {
        this.vector = null;
    }
    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        if (args.length < 2) {
            System.err.println("USAGE: PSWorker workerNumber [host:port ...]");
            System.exit(2);
        }

        int workerId = Integer.parseInt(args[0]);
        String addrs = args[1];
        for (int i = 2; i < args.length; i++)
            addrs += "," + args[i];
        ZooKeeper zk = new ZooKeeper(addrs, 3000, null);
        PSWorker worker = new PSWorker();
        while(zk.exists("/start", false) == null);
        zk.getData("/m", true, worker, null);

        // 1. Execute training script
        ProcessBuilder pb = new ProcessBuilder("python", "train.py");
        Process process = pb.start();
        if (process.waitFor() != 0)
            return;

        // 2. Read params
        List<Float> params = new ArrayList<Float>();
        try {
            File file = new File("params.txt");
            Scanner scanner = new Scanner(file);
            while (scanner.hasNextFloat()) {
                params.add(scanner.nextFloat());
            }
            file.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 3. Convert params to byte array and write the data to znode with the same
        // workerId
        byte[] vector = new byte[params.size() * 4];
        for (int i = 0; i < params.size(); i++) {
            byte[] byteRep = ByteBuffer.allocate(4).putFloat(params.get(i)).array();
            for (int j = 0; j < 4; j++)
                vector[4 * i + j] = byteRep[j];
        }
        zk.setData("/w" + workerId, vector, -1);
    }

    // 4. Get the average gradient from the manager znode
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        if (rc != Code.OK.intValue()) {
            System.out.println("Something's wrong");
            return;
        }

        this.vector = new float[data.length / 4];
        for (int j = 0; j < this.vector.length; j++)
            this.vector[j] = ByteBuffer.wrap(data, 4 * j, 4).getFloat();
    }
}
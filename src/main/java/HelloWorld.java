import org.apache.ignite.*;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import java.util.Collections;
import java.util.Arrays;
import java.util.Comparator;

public class HelloWorld {
    public static void main(String[] args) throws IgniteException {
        int n = 10;

        int [][] matrix = genMatrix(n);

        // Preparing IgniteConfiguration using Java APIs
        IgniteConfiguration cfg = new IgniteConfiguration();

        // The node will be started as a client node.
        cfg.setClientMode(true);

        // Classes of custom Java logic will be transferred over the wire from this app.
        cfg.setPeerClassLoadingEnabled(true);

        // Setting up an IP Finder to ensure the client can locate the servers.
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        // Starting the node
        Ignite ignite = Ignition.start(cfg);

        LogDuration time = new LogDuration("Grid Gain Apache Ignite");
        time.start();

        // Create an IgniteCache and put some values in it.
        IgniteCache<Integer, int[]> cache = ignite.getOrCreateCache("MatrixCache");
        for (int i = 0; i < n; i++){
            cache.put(i, matrix[i]);
        }

        System.out.println(">> Created the cache and add the values.");

        // Executing custom Java compute task on server nodes.

        IgniteCompute computeCluster =  ignite.compute(ignite.cluster().forServers());

        for (int i = 0; i < n; i++) {
            computeCluster.run(new RemoteTask(i));
        }

        System.out.println(">> Compute task is executed, check for output on the server nodes.");

        for (int i = 0; i < n; i++) {
            matrix[i] = cache.get(i);
        }

        Arrays.sort(matrix, new Comparator<int[]>() {
            @Override
            public int compare(int[] o1, int[] o2) {
                return Integer.compare(o2[n], o1[n]);
            }
        });

        time.end();

        System.out.println(">> Result:");

        System.out.println(Arrays.deepToString(matrix));

        // Disconnect from the cluster.
        ignite.close();
    }

    /**
     * A compute tasks that prints out a node ID and some details about its OS and JRE.
     * Plus, the code shows how to access data stored in a cache from the compute task.
     */
    private static class RemoteTask implements IgniteRunnable {
        @IgniteInstanceResource
        Ignite ignite;

        int index;

        RemoteTask(int index) {
            this.index = index;
        }

        @Override public void run() {
            System.out.println(">> Executing the compute task: " + index + "\n");

            System.out.println(
                    "   Node ID: " + ignite.cluster().localNode().id() + "\n" +
                    "   OS: " + System.getProperty("os.name") +
                    "   JRE: " + System.getProperty("java.runtime.name"));

            IgniteCache<Integer, int[]> cache = ignite.cache("MatrixCache");

            int[] row = cache.get(index);
            row[row.length - 1] = Arrays.stream(row).sum();

            cache.put(index, row);

            System.out.println(">> " + index + " complete " + Arrays.toString(cache.get(index)));
        }
    }

    private static int [][] genMatrix(int n ) {
        int [][] matrix = new int [n][n + 1];
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n + 1; ++j) {
                matrix[i][j] = (int) (Math.random() * 10000);
                if (n == j) {
                    matrix[i][j] = 0;
                }
            }
        }
        return matrix;
    }
}
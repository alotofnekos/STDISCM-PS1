import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class GraphProcessor {
    
    private static class Graph {
        private final Set<String> nodes = new HashSet<>();
        private final List<String[]> edges = new ArrayList<>();

        public void addNode(String node) {
            nodes.add(node);
        }

        public void addEdge(String source, String target) {
            if (nodes.contains(source) && nodes.contains(target)) {
                edges.add(new String[]{source, target});
            }
        }

        public Set<String> getNodes() {
            return nodes;
        }

        public List<String[]> getEdges() {
            return edges;
        }
        
        public List<String> getOutNeighbors(String node) {
            List<String> neighbors = new ArrayList<>();
            for (String[] edge : edges) {
                if (edge[0].equals(node)) {
                    neighbors.add(edge[1]);
                }
            }
            return neighbors;
        }
    }

    private static final Graph graph = new Graph();
    
    public static void main(String[] args) {
        parseGraphCLI(args);
    }

    // Parse graph, then get user input
    public static void parseGraphCLI(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the graph file name: ");
        String filename = scanner.nextLine();

        long startTime = System.nanoTime();

        try (Stream<String> lines = Files.lines(Paths.get(filename))) {
            lines.forEachOrdered(line -> { 
                String trimmedLine = line.trim();
                if (trimmedLine.isEmpty()) return;

                if (trimmedLine.startsWith("*")) {
                    String node = trimmedLine.substring(1).trim();
                    graph.addNode(node);
                } else if (trimmedLine.startsWith("-")) {
                    String[] parts = trimmedLine.substring(1).trim().split("\\s+");

                    if (parts.length != 2) { 
                        System.err.println("Malformed edge line: " + trimmedLine);
                        return;
                    }

                    String source = parts[0];
                    String target = parts[1];
                    graph.addEdge(source, target);
                }
            });

            System.out.println("Total unique nodes: " + graph.getNodes().size());
            System.out.println("Total edges: " + graph.getEdges().size());

        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }

        long endTime = System.nanoTime();
        System.out.println("Graph parsed in " + (endTime - startTime) / 1_000_000 + " ms");
        
        // CLI Commands
        while (true) {
            System.out.print("\nEnter your query (nodes, node x, edges, edge a b, path a b, exit): ");
            String query = scanner.nextLine().trim();
            if (query.equalsIgnoreCase("exit")) {
                System.out.println("Exiting the program.");
                break;
            } else if (query.equalsIgnoreCase("nodes")) {
                System.out.println("Nodes: " + graph.getNodes());
            } else if (query.startsWith("node ")) {
                String node = query.substring(5).trim();
                long startTimeSingle = System.nanoTime();
                boolean existsSingle = nodeSearch(node);
                long endTimeSingle = System.nanoTime();
                long timeSingle = endTimeSingle - startTimeSingle;               
                // Measure time for Multi-threaded search
                long startTimeThreaded = System.nanoTime();
                boolean existsThreaded = parallelNodeSearch(node);
                long endTimeThreaded = System.nanoTime();
                long timeThreaded = endTimeThreaded - startTimeThreaded;
                System.out.println("Single-threaded search: " + (existsSingle ? "Node exists." : "Node does not exist.") +
                        " Time taken: " + timeSingle + " ns");
                
                System.out.println("Multi-threaded search: " + (existsThreaded ? "Node exists." : "Node does not exist.") +
                        " Time taken: " + timeThreaded + " ns");
            } else if (query.equalsIgnoreCase("edges")) {
                System.out.println("Edges: " + graph.getEdges());
            } else if (query.startsWith("edge ")) {
                String[] parts = query.substring(5).trim().split("\\s+");
                if (parts.length == 2) {
                    String source = parts[0], target = parts[1];
                    long startTimeSingle = System.nanoTime();
                    boolean existsSingle = edgeSearch(source, target);
                    long endTimeSingle = System.nanoTime();
                    long timeSingle = endTimeSingle - startTimeSingle;
                    long startTimeThreaded = System.nanoTime();
                    boolean existsThreaded = parallelEdgeSearch(source, target);
                    long endTimeThreaded = System.nanoTime();
                    long timeThreaded = endTimeThreaded - startTimeThreaded;
                    System.out.println("Single-threaded search: " + (existsSingle ? "Edge exists." : "Edge does not exist.") +
                            " Time taken: " + timeSingle + " ns");
                    
                    System.out.println("Multi-threaded search: " + (existsThreaded ? "Edge exists." : "Edge does not exist.") +
                            " Time taken: " + timeThreaded + " ns");
                    
                    
                } else {
                    System.out.println("Invalid edge query format. Use 'edge a b'.");
                }
            } else if (query.startsWith("path ")) {
                String[] parts = query.substring(5).trim().split("\\s+");
                if (parts.length == 2) {
                    String start = parts[0], end = parts[1];

                    // Sequential Path Search
                    long startTimeSeq = System.currentTimeMillis();
                    List<String> seqPath = sequentialPathSearch(start, end);
                    long endTimeSeq = System.currentTimeMillis();
                    long seqTime = (endTimeSeq - startTimeSeq);
                    // Parallel Path Search
                    long startTimePar = System.currentTimeMillis();
                    List<String> parPath = parallelPathSearch(start, end);
                    long endTimePar = System.currentTimeMillis();
                    long parTime = (endTimePar - startTimePar);

                    System.out.println("Sequential Path Search: " + (!seqPath.isEmpty() ? String.join(" -> ", seqPath) : "No path found") + " (Time: " + seqTime + " ms)");
                    System.out.println("Parallel Path Search: " +  (!parPath.isEmpty() ? String.join(" -> ", parPath) : "No path found") + " (Time: " + parTime + " ms)");
                } else {
                    System.out.println("Invalid path query format. Use 'path a b'.");
                }
            } else {
                System.out.println("Unknown command.");
            }
        }
    }

    // Single-threaded node search
    public static boolean nodeSearch(String node) {
        return graph.getNodes().contains(node);
    }

    public static boolean normalSearch(List<String> nodes, String node) {
        for (String n : nodes) {  
            if (n.equals(node)) { 
                return true; 
            }
        }
        return false; 
    }
    
    // Single-threaded edge search
    public static boolean edgeSearch(String source, String target) {
        for (String[] edge : graph.getEdges()) {
            if (edge[0].equals(source) && edge[1].equals(target)) {
                return true; 
            }
        }
        return false; 
    }
    
    // Multi-threaded node search
    public static boolean parallelNodeSearch(String node) {
        List<String> nodesList = new ArrayList<>(graph.getNodes());
    
        int numThreads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Boolean>> results = new ArrayList<>();
    
        int chunkSize = nodesList.size() / numThreads;
    
        for (int i = 0; i < numThreads; i++) {
            int start = i * chunkSize;
            int end = (i == numThreads - 1) ? nodesList.size() : (start + chunkSize);
            List<String> sublist = nodesList.subList(start, end);
    
            results.add(executor.submit(() -> normalSearch(sublist, node)));
        }
    
        boolean found = false;
        for (Future<Boolean> result : results) {
            try {
                if (result.get()) {
                    found = true;
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    
        executor.shutdown();
        return found;
    }

    // Multi-threaded edge search
    public static boolean parallelEdgeSearch(String source, String target) {
        List<String[]> edgeList = new ArrayList<>(graph.getEdges());  
        int numThreads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Boolean>> results = new ArrayList<>();

        int chunkSize = edgeList.size() / numThreads;
        
        for (int i = 0; i < numThreads; i++) {
            final int start = i * chunkSize;
            final int end = (i == numThreads - 1) ? edgeList.size() : (start + chunkSize);
            
            Callable<Boolean> task = () -> {
                for (int j = start; j < end; j++) {
                    String[] e = edgeList.get(j);
                    if (e[0].equals(source) && e[1].equals(target)) {
                        return true;
                    }
                }
                return false;
            };

            results.add(executor.submit(task));
        }

        executor.shutdown();

        try {
            for (Future<Boolean> result : results) {
                if (result.get()) { 
                    return true;
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return false;
    }

    // Sequential DFS path search
    public static List<String> sequentialPathSearch(String start, String end) {
        Set<String> visited = new HashSet<>();
        List<String> initialPath = new ArrayList<>();
        initialPath.add(start);
        visited.add(start);
        AtomicBoolean found = new AtomicBoolean(false);
        
        List<String> result = searchPath(start, end, visited, initialPath, found);
        
        return result != null ? result : new ArrayList<>();
    }

    private static List<String> searchPath(String current, String target,
                                        Set<String> visited, List<String> currentPath,
                                        AtomicBoolean found) {
        if (found.get()) return null;
        
        if (current.equals(target)) {
            return new ArrayList<>(currentPath);
        }

        for (String neighbor : graph.getOutNeighbors(current)) {
            if (found.get()) return null;
            
            if (visited.add(neighbor)) {  
                List<String> newPath = new ArrayList<>(currentPath);
                newPath.add(neighbor);
                
                List<String> result = searchPath(neighbor, target, visited, newPath, found);
                if (result != null) {
                    return result;
                }
            }
        }

        return null;
    }

    // Parallel DFS path search
    public static List<String> parallelPathSearch(String start, String end) {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CompletionService<List<String>> completionService = new ExecutorCompletionService<>(executor);
        Set<String> visited = Collections.synchronizedSet(new HashSet<>());
        AtomicReference<List<String>> finalPath = new AtomicReference<>(null);
        AtomicBoolean found = new AtomicBoolean(false);
        visited.add(start);
        
        AtomicInteger activeTasks = new AtomicInteger(0); // Track active tasks
    
        // Submit initial task
        activeTasks.incrementAndGet();
        completionService.submit(() -> searchPathParallel(start, end, visited, new ArrayList<>(List.of(start)), found, completionService, activeTasks));
    
        try {
            while (activeTasks.get() > 0) {
                Future<List<String>> future = completionService.poll(60, TimeUnit.SECONDS);
                if (future != null) {
                    List<String> result = future.get();
                    activeTasks.decrementAndGet();
    
                    if (result != null) {
                        finalPath.set(result);
                        found.set(true);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdownNow();
        }
    
        return finalPath.get() != null ? finalPath.get() : new ArrayList<>();
    }
    
    // The parallel recursive DFS method
    private static List<String> searchPathParallel(String current, String target,
                                                   Set<String> visited, List<String> currentPath,
                                                   AtomicBoolean found, CompletionService<List<String>> completionService,
                                                   AtomicInteger activeTasks) {
        if (found.get()) return null;
        if (current.equals(target)) return new ArrayList<>(currentPath);
    
        for (String neighbor : graph.getOutNeighbors(current)) {
            if (found.get()) return null;
            
            if (visited.add(neighbor)) { 
                List<String> newPath = new ArrayList<>(currentPath);
                newPath.add(neighbor);
                activeTasks.incrementAndGet();
                completionService.submit(() -> searchPathParallel(neighbor, target, visited, newPath, found, completionService, activeTasks));
            }
        }
        return null;
    }
}

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Graphmaker {
    public static void main(String[] args) {
        int numNodes = 100000; 
        int numEdges = 200000;  

        generateGraphFile("graph2.txt", numNodes, numEdges);
    }

    public static void generateGraphFile(String filename, int numNodes, int numEdges) {
        try (FileWriter writer = new FileWriter(filename)) {
            // Write nodes
            for (int i = 0; i < numNodes; i++) {
                writer.write("* N" + i + "\n");
            }

            // Use HashSet to ensure unique edges
            Random rand = new Random();
            Set<String> edges = new HashSet<>();
            
            while (edges.size() < numEdges) {
                int a = rand.nextInt(numNodes);
                int b = rand.nextInt(numNodes);
                if (a != b) { 
                    String edge = a < b ? "- N" + a + " N" + b : "- N" + b + " N" + a;
                    if (edges.add(edge)) { 
                        writer.write(edge + "\n");
                    }
                }
            }

            System.out.println("Graph file generated successfully: " + filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

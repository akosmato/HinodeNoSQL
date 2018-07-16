package edu.csd.auth.models;

import com.datastax.driver.core.ConsistencyLevel;
import edu.csd.auth.utils.DiaNode;
import edu.csd.auth.utils.SnapshotResult;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public interface DataModel
{  
    static final ConsistencyLevel READ_CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
    static final ConsistencyLevel WRITE_CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
    
    public abstract void createSchema();
    public void useKeyspace();
    public abstract void parseInput(String input);

    public abstract void insertVertex(String vid, String name, String start, String end, String color);
    public abstract void insertEdge(String sourceID, String targetID, String start, String end, String label, String weight);
    public abstract void updateVertexAttribute(String vid, String attrName, String attrValue, String timestamp);
    
    public abstract DiaNode getVertexHistory(String vid, String first, String last);
    
    public List<String> getOneHopNeighborhood(String vid, String first, String last); // We only want the one-hop list of vIDs, not the vertices themselves
    
    public List<SnapshotResult> getAvgVertexDegree(String first, String last);
    public List<SnapshotResult> getAvgVertexDegreeFetchAllVertices(String first, String last);
    
    public HashMap<String, HashMap<String, Integer>> getDegreeDistribution(String first, String last);
    public HashMap<String, HashMap<String, Integer>> getDegreeDistributionFetchAllVertices(String first, String last);

    static public int getCountOfSnapshotsInInput(String input) 
    {
        int count = -1;
        try 
        {
            String line;
            String tokens[] = null;
            BufferedReader file = new BufferedReader(new FileReader(input));
            while ((line = file.readLine()) != null)
                if (line.startsWith("graph"))
                    tokens = line.split(" ");
                    count = Integer.parseInt(tokens[1]);
            file.close();
        } catch (FileNotFoundException ex) 
        {
            Logger.getLogger(BaselineModel.class.getName()).log(Level.SEVERE, null, ex);
        } 
        catch (IOException ex) 
        {
            Logger.getLogger(BaselineModel.class.getName()).log(Level.SEVERE, null, ex);
        }
        return count;
    }
    
    static public String getRandomString (int len)
    {
        String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        Random rnd = new Random();
        
        StringBuilder sb = new StringBuilder( len );
        for( int i = 0; i < len; i++ ) 
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();
    }
    
    static public String padWithZeros(String timestamp)
    {
        return String.format("%08d", Integer.parseInt(timestamp));
    }
}

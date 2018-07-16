package edu.csd.auth.models;

import com.datastax.driver.core.Session;
import static edu.csd.auth.models.DataModel.getRandomString;
import edu.csd.auth.utils.DiaNode;
import edu.csd.auth.utils.Edge;
import edu.csd.auth.utils.Interval;
import edu.csd.auth.utils.SnapshotResult;
import edu.csd.auth.utils.Vertex;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BaselineModel implements DataModel
{
    private String keyspace = null;
    private Session session = null;
    
    private final int SNAPSHOT_GROUPCOUNT_THRESHOLD = 20;
    
    private final SingleTableModel st_model;
    
    public BaselineModel(Session session, String keyspace) 
    {
        this.session = session;
        this.keyspace = keyspace;
        st_model = new SingleTableModel(session, keyspace);
    }
    
    @Override
    public void createSchema() 
    {
        st_model.createSchema();
    }

    private List<Interval> getAllQueryIntervals(String first, String last) 
    {
        int start = Integer.parseInt(first);
        int end = Integer.parseInt(last);
        
        List<Interval> intervals = new ArrayList<Interval>();
        Interval ival = new Interval();
        ival.start = ""+start;
        
        for (int i=start; i<end; i++)
            if (i % SNAPSHOT_GROUPCOUNT_THRESHOLD == 0)
            {
                ival.end = ""+i;
                if (ival.start.equals(ival.end))
                    continue;
                intervals.add(ival);
                ival = new Interval();
                ival.start = ""+i;
            }
        ival.end = ""+end;
        intervals.add(ival);
        
        return intervals;
        
    }

    @Override
    public List<SnapshotResult> getAvgVertexDegree(String first, String last) 
    {
        List<SnapshotResult> results = new ArrayList<SnapshotResult>();
        
        List<Interval> intervals = getAllQueryIntervals(first, last); 
        Iterator<Interval> it = intervals.iterator();
        
        while (it.hasNext())
        {
            Interval ival = it.next();
            if (it.hasNext()) // This is not the last interval in the list
                results.addAll(st_model.getAvgVertexDegree(ival.start, ""+(Integer.parseInt(ival.end)-1))); 
            else
            {
                if (Integer.parseInt(ival.end) % SNAPSHOT_GROUPCOUNT_THRESHOLD == 0) // This is the last interval AND it's a multiplier of SNAPSHOT_GROUPCOUNT_THRESHOLD
                {
                   results.addAll(st_model.getAvgVertexDegree(ival.start, ""+(Integer.parseInt(ival.end)-1)));
                   results.addAll(st_model.getAvgVertexDegree(ival.end, ival.end));
                }
                else // This is the last interval but it's NOT a multiplier of SNAPSHOT_GROUPCOUNT_THRESHOLD
                {
                    results.addAll(st_model.getAvgVertexDegree(ival.start, ival.end));
                }
            }
        }
        
        return results;
    }

    @Override
    public List<SnapshotResult> getAvgVertexDegreeFetchAllVertices(String first, String last) 
    {
        List<SnapshotResult> results = new ArrayList<SnapshotResult>();
        
        List<Interval> intervals = getAllQueryIntervals(first, last); 
        Iterator<Interval> it = intervals.iterator();
        
        while (it.hasNext())
        {
            Interval ival = it.next();
            if (it.hasNext()) // This is not the last interval in the list
                results.addAll(st_model.getAvgVertexDegreeFetchAllVertices(ival.start, ""+(Integer.parseInt(ival.end)-1))); 
            else
            {
                if (Integer.parseInt(ival.end) % SNAPSHOT_GROUPCOUNT_THRESHOLD == 0) // This is the last interval AND it's a multiplier of SNAPSHOT_GROUPCOUNT_THRESHOLD
                {
                   results.addAll(st_model.getAvgVertexDegreeFetchAllVertices(ival.start, ""+(Integer.parseInt(ival.end)-1)));
                   results.addAll(st_model.getAvgVertexDegreeFetchAllVertices(ival.end, ival.end));
                }
                else // This is the last interval but it's NOT a multiplier of SNAPSHOT_GROUPCOUNT_THRESHOLD
                {
                    results.addAll(st_model.getAvgVertexDegreeFetchAllVertices(ival.start, ival.end));
                }
            }
        }
        
        return results;
    }

    @Override
    public HashMap<String, HashMap<String, Integer>> getDegreeDistribution(String first, String last) 
    {
        HashMap<String, HashMap<String, Integer>> results = new HashMap<String, HashMap<String, Integer>>();

        List<Interval> intervals = getAllQueryIntervals(first, last);
        Iterator<Interval> it = intervals.iterator();

        while (it.hasNext()) 
        {
            Interval ival = it.next();
            if (it.hasNext()) // This is not the last interval in the list
                results.putAll(st_model.getDegreeDistribution(ival.start, "" + (Integer.parseInt(ival.end) - 1)));
            else
            {
                if (Integer.parseInt(ival.end) % SNAPSHOT_GROUPCOUNT_THRESHOLD == 0) // This is the last interval AND it's a multiplier of SNAPSHOT_GROUPCOUNT_THRESHOLD
                {
                    results.putAll(st_model.getDegreeDistribution(ival.start, "" + (Integer.parseInt(ival.end) - 1)));
                    results.putAll(st_model.getDegreeDistribution(ival.end, ival.end));
                } 
                else // This is the last interval but it's NOT a multiplier of SNAPSHOT_GROUPCOUNT_THRESHOLD
                {
                    results.putAll(st_model.getDegreeDistribution(ival.start, ival.end));
                }
            }
        }

        return results;
    }

    @Override
    public HashMap<String, HashMap<String, Integer>> getDegreeDistributionFetchAllVertices(String first, String last) 
    {
        HashMap<String, HashMap<String, Integer>> results = new HashMap<String, HashMap<String, Integer>>();

        List<Interval> intervals = getAllQueryIntervals(first, last);
        Iterator<Interval> it = intervals.iterator();

        while (it.hasNext()) 
        {
            Interval ival = it.next();
            if (it.hasNext()) // This is not the last interval in the list
                results.putAll(st_model.getDegreeDistributionFetchAllVertices(ival.start, "" + (Integer.parseInt(ival.end) - 1)));
            else
            {
                if (Integer.parseInt(ival.end) % SNAPSHOT_GROUPCOUNT_THRESHOLD == 0) // This is the last interval AND it's a multiplier of SNAPSHOT_GROUPCOUNT_THRESHOLD
                {
                    results.putAll(st_model.getDegreeDistributionFetchAllVertices(ival.start, "" + (Integer.parseInt(ival.end) - 1)));
                    results.putAll(st_model.getDegreeDistributionFetchAllVertices(ival.end, ival.end));
                } 
                else // This is the last interval but it's NOT a multiplier of SNAPSHOT_GROUPCOUNT_THRESHOLD
                {
                    results.putAll(st_model.getDegreeDistributionFetchAllVertices(ival.start, ival.end));
                }
            }
        }

        return results;
    }

    @Override
    public List<String> getOneHopNeighborhood(String vid, String first, String last)
    {
        List<Interval> intervals = getAllQueryIntervals(first, last);
        Iterator<Interval> it = intervals.iterator();
        
        List<String> results = new ArrayList<String>();
        
        while (it.hasNext()) 
        {
            Interval ival = it.next();
            
            int time_id = (Integer.parseInt(ival.end) - 1) / SNAPSHOT_GROUPCOUNT_THRESHOLD;
            String newVid = vid + "_" + time_id;
            
            if (it.hasNext()) // This is not the last interval in the list
                results.addAll(st_model.getOneHopNeighborhood(newVid, ival.start, "" + (Integer.parseInt(ival.end) - 1)));
            else 
            {
                if (Integer.parseInt(ival.end) % SNAPSHOT_GROUPCOUNT_THRESHOLD == 0) // This is the last interval AND it's a multiplier of SNAPSHOT_GROUPCOUNT_THRESHOLD
                {
                    results.addAll(st_model.getOneHopNeighborhood(newVid, ival.start, "" + (Integer.parseInt(ival.end) - 1)));
                    results.addAll(st_model.getOneHopNeighborhood(newVid, ival.end, "" + ival.end));
                } 
                else // This is the last interval but it's NOT a multiplier of SNAPSHOT_GROUPCOUNT_THRESHOLD
                    results.addAll(st_model.getOneHopNeighborhood(newVid, ival.start, ival.end));
            }
        }

        return results;
    }

    @Override
    public DiaNode getVertexHistory(String vid, String first, String last) 
    {
        List<Interval> intervals = getAllQueryIntervals(first, last);
        Iterator<Interval> it = intervals.iterator();

        DiaNode dn = new DiaNode(vid);
        
        while (it.hasNext()) 
        {
            int time_id = (Integer.parseInt(it.next().end)-1) / SNAPSHOT_GROUPCOUNT_THRESHOLD;
            String newVid = vid + "_" + time_id;
            
            dn.merge(st_model.getVertexHistory(newVid, first, last));
            
        }
        
        return dn;
    }

    @Override
    public void insertEdge(String sourceID, String targetID, String start, String end, String label, String weight) 
    {
        List<Interval> intervals = getAllQueryIntervals(start, end);
        
        for (Interval ival : intervals)
        {
            String time_group_id = "" + (Integer.parseInt(ival.start) / SNAPSHOT_GROUPCOUNT_THRESHOLD);
            st_model.insertEdge(sourceID+"_"+time_group_id, targetID+"_"+time_group_id, ival.start, ival.end, label, weight);
        }
    }

    @Override
    public void insertVertex(String vid, String name, String start, String end, String color) 
    {
        List<Interval> intervals = getAllQueryIntervals(start, end);
        
        for (Interval ival : intervals)
        {
            String time_group_id = "" + (Integer.parseInt(ival.start) / SNAPSHOT_GROUPCOUNT_THRESHOLD);
            st_model.insertVertex(vid+"_"+time_group_id, name, ival.start, ival.end, color);
        }        
    }

    private void parseFirstSnapshot(String input, int snap_count) // Used to bulk load data instead of using the typical methods
    {     
        try
        {   
            BufferedReader file = new BufferedReader(new FileReader(input));
            String line;
            String tokens[];
            
            TreeMap<String, Vertex> vertices = new TreeMap<String, Vertex>();
            
            while ((line = file.readLine()) != null)
            {
                if (line.startsWith("mkdir") || line.startsWith("cd") || line.startsWith("time") || line.startsWith("string") || line.startsWith("double") || line.startsWith("shutdown"))
                    continue;
                
                if (line.equals("graph 1 0") || line.equals("graph 1")) // As soon as we've reached the second snapshot, stop
                    break;
                
                if (line.equals("use "+keyspace+""))
                    session.execute("USE "+keyspace+";");
                
                else if (line.startsWith("vertex"))
                {
                    tokens = line.split(" ");
                    String verID = tokens[1];
                    String name, color;
                    if (tokens.length >= 3)
                        name = tokens[2].split("=")[1].replaceAll("\"", "");
                    else
                        name = getRandomString(4);
                    if (tokens.length == 4)
                        color = tokens[3].split("=")[1].replaceAll("\"", "");
                    else
                        color = getRandomString(4);
                    Vertex ver = new Vertex();
                    ver.setVid(verID);
                    ver.setTimestamp("00000000");
                    HashMap<String, String> attributes = new HashMap<String, String>();
                    attributes.put("name", name);
                    attributes.put("color", color);
                    ver.setAttributes(attributes);
                    vertices.put(verID, ver);
                }
                else if (line.startsWith("edge"))
                {
                    tokens = line.split(" ");
                    String sourceID = tokens[1];
                    String targetID = tokens[2];
                    String weight;
                    if (tokens.length == 4)
                        weight = tokens[3].split("=")[1];
                    else
                        weight = ""+Math.random();

                    Vertex sVer = vertices.get(sourceID);
                    sVer.addOutgoingEdge(targetID, new Edge("testlabel", weight, targetID, "00000000", DataModel.padWithZeros(""+snap_count)));
                    Vertex tVer = vertices.get(targetID);
                    tVer.addIncomingEdge(sourceID, new Edge("testlabel", weight, sourceID, "00000000", DataModel.padWithZeros(""+snap_count)));
                    vertices.put(sourceID, sVer);
                    vertices.put(targetID, tVer);
                }
            }
            file.close();
            
            for (String vid : vertices.keySet())
            {
                Vertex ver = vertices.get(vid);
                HashMap<String, String> attrs = ver.getAttributes();
                
                String start = ver.getTimestamp();
                String end = DataModel.padWithZeros(""+snap_count);
                String name = attrs.get("name");
                String color = attrs.get("color");
                
                HashMap<String, Edge> allIncEdges = ver.getIncoming_edges();
                HashMap<String, Edge> allOutEdges = ver.getOutgoing_edges();
                
                String allIncEdgesstr = "";
                for (String source : allIncEdges.keySet())
                {
                    Edge edge = allIncEdges.get(source);
                    allIncEdgesstr = allIncEdgesstr.concat("'" + edge.otherEnd + "': [");
                    allIncEdgesstr = allIncEdgesstr.concat(edge.toString()).concat("], ");
                }
                if (!allIncEdgesstr.equals(""))
                    allIncEdgesstr = allIncEdgesstr.substring(0, allIncEdgesstr.length()-2);


                String allOutEdgesstr = "";
                for (String target : allOutEdges.keySet())
                {
                    Edge edge = allOutEdges.get(target);
                    allOutEdgesstr = allOutEdgesstr.concat("'" + edge.otherEnd + "': [");
                    allOutEdgesstr = allOutEdgesstr.concat(edge.toString()).concat("], ");
                }
                if (!allOutEdgesstr.equals(""))
                    allOutEdgesstr = allOutEdgesstr.substring(0, allOutEdgesstr.length() - 2);

                List<Interval> intervals = getAllQueryIntervals(start, end);
        
                for (Interval ival : intervals)
                {
                    String time_group_id = "" + (Integer.parseInt(ival.start) / SNAPSHOT_GROUPCOUNT_THRESHOLD);
       
                    session.execute("INSERT INTO "+keyspace+".dianode (vid, start, end, name, color, incoming_edges, outgoing_edges)" //Assume only one edge per vertex pair in graph 0
                            + "VALUES ('" + ver.getVid()+"_"+time_group_id + "', '" + DataModel.padWithZeros(""+ival.start) + "', '" + DataModel.padWithZeros(""+ival.end) + "', "
                                    + "[{value: '" + name + "', start: '00000000', end: '" + DataModel.padWithZeros(""+snap_count) + "'}], "
                                            + "[{value: '" + color + "', start: '00000000', end: '" + DataModel.padWithZeros(""+snap_count) + "'}], "
                                                    + "{" + allIncEdgesstr + "}, "
                                                            + "{" + allOutEdgesstr + "}"
                                                                    + ");");
                }
            }
        }
        catch (FileNotFoundException ex)
        {
            Logger.getLogger(SingleTableModel.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (IOException ex)
        {
            Logger.getLogger(SingleTableModel.class.getName()).log(Level.SEVERE, null, ex);
        }
    }    
    
    @Override
    public void parseInput(String input) 
    {
        try
        {
            int snap_count = DataModel.getCountOfSnapshotsInInput(input);

            parseFirstSnapshot(input, snap_count);
            
            BufferedReader file = new BufferedReader(new FileReader(input));
            String line, curVersion = "0";
            String tokens[];
            int verKcounter = 0;
            int edgeKcounter = 0;
            
            boolean passedFirstSnapshot = false;
            
            while ((line = file.readLine()) != null)
            {
                if (line.equals("graph 1 0") || line.equals("graph 1"))
                    passedFirstSnapshot = true;
                
                if (!passedFirstSnapshot || line.startsWith("mkdir") || line.startsWith("cd") || line.startsWith("time") || line.startsWith("string") || line.startsWith("double") || line.startsWith("shutdown"))
                    continue;
                
                if (line.equals("use "+keyspace+""))
                    session.execute("USE "+keyspace+";");
                
                if (line.startsWith("graph"))
                {
                    System.out.println(line);
                    tokens = line.split(" ");
                    if (tokens.length == 2) // "graph X" statement
                        curVersion = tokens[1];
                    else if (tokens.length == 3) // "graph X Y" statement
                        curVersion = tokens[1];
                }
                else if (line.startsWith("vertex"))
                {
                    tokens = line.split(" ");
                    String verID = tokens[1];
                    String name, color;
                    if (tokens.length >= 3)
                        name = tokens[2].split("=")[1].replaceAll("\"", "");
                    else
                        name = getRandomString(4);
                    if (tokens.length == 4)
                        color = tokens[3].split("=")[1].replaceAll("\"", "");
                    else
                        color = getRandomString(4);
                    insertVertex(verID, name, DataModel.padWithZeros(curVersion), DataModel.padWithZeros(""+snap_count), color);
                    verKcounter++;
                    if (verKcounter % 1000 == 0)
                        System.out.println("Vertices processed: " + verKcounter);
                }
                else if (line.startsWith("edge"))
                {
                    tokens = line.split(" ");
                    String sourceID = tokens[1];
                    String targetID = tokens[2];
                    String weight;
                    if (tokens.length == 4)
                        weight = tokens[3].split("=")[1];
                    else
                        weight = ""+Math.random();
                    insertEdge(sourceID, targetID, DataModel.padWithZeros(curVersion), DataModel.padWithZeros(""+snap_count), getRandomString(3), weight);
                    edgeKcounter++;
                    if (edgeKcounter % 1000 == 0)
                        System.out.println("Edges processed: " + edgeKcounter);
                }
                else if (line.startsWith("update vertex"))
                {
                    tokens = line.split(" ");
                    String verID = tokens[2];
                    String attrName = tokens[3].split("=")[0];
                    String value = tokens[3].split("=")[1].replaceAll("\"", "");
                    updateVertexAttribute(verID, attrName, value, DataModel.padWithZeros(curVersion));
                }
            }
        }
        catch (FileNotFoundException ex)
        {
            Logger.getLogger(SingleTableModel.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (IOException ex)
        {
            Logger.getLogger(SingleTableModel.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void updateVertexAttribute(String vid, String attrName, String attrValue, String timestamp) 
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void useKeyspace() 
    {
        this.session.execute("USE "+keyspace+";");
    }
    
}

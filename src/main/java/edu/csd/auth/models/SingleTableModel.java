package edu.csd.auth.models;

import edu.csd.auth.utils.Edge;
import edu.csd.auth.utils.Interval;
import edu.csd.auth.utils.DiaNode;
import edu.csd.auth.utils.Vertex;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.google.common.reflect.TypeToken;
import static edu.csd.auth.models.DataModel.getRandomString;
import edu.csd.auth.utils.SnapshotResult;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SingleTableModel implements DataModel
{
    private String keyspace = null;
    private Session session = null;
    
    public static String bb_to_str(ByteBuffer buffer)
    {
        String data;
        Charset charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = charset.newDecoder();
        try
        {
            int old_position = buffer.position();
            data = decoder.decode(buffer).toString();
            // reset buffer's position to its original so it is not altered:
            buffer.position(old_position);
        }
        catch (CharacterCodingException e)
        {
            e.printStackTrace();
            return "";
        }
        return data;
    }
    public static Map<String, List<Edge>> convertToEdgeList(Map<String, List<UDTValue>> edgesUDTList)
    {
        if (edgesUDTList == null)
            return Collections.EMPTY_MAP;
        
        Map<String, List<Edge>> edges = new HashMap<String, List<Edge>>();
        
        for (String ver : edgesUDTList.keySet())
        {
            List<UDTValue> edgesUDT = edgesUDTList.get(ver);
            List<Edge> vertex_edges = new ArrayList<Edge>();
            
            for (UDTValue udt : edgesUDT)
            {
                Edge edge = new Edge("temp", udt.get("weight", String.class), udt.get("otherEnd", String.class), udt.get("start", String.class), udt.get("end", String.class));
                vertex_edges.add(edge);
            }
            edges.put(ver, vertex_edges);
        }
        
        return edges;
    }
    public static List<Interval> convertToIntervals(List<UDTValue> nameUDTList)
    {
        List<Interval> list = new ArrayList<Interval>();
        
        for (UDTValue val : nameUDTList)
            list.add(new Interval(val.get("value", String.class),val.get("start", String.class),val.get("end", String.class)));
        
        Collections.sort(list);
        return list;
    }
    
    public SingleTableModel(Session session, String keyspace)
    {
        this.session = session;
        this.keyspace = keyspace;
    }
    
    @Override
    public void createSchema()
    {
        try
        {
            session.execute("DROP KEYSPACE IF EXISTS "+keyspace+";");
            Thread.sleep(30000);
            
            session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspace+" WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':1};");
            Thread.sleep(5000);
            
            session.execute("USE "+keyspace+";");
            Thread.sleep(5000);
            
            session.execute("CREATE TYPE "+keyspace+".attribute ("
                    + " value text,"
                    + " start text,"
                    + " end text"
                    + ");");
            Thread.sleep(10000);
            session.execute("CREATE TYPE "+keyspace+".edge ("
                    + " label text,"
                    + " weight text,"
                    + " otherEnd text,"
                    + " start text,"
                    + " end text"
                    + ");");
            Thread.sleep(10000);
            session.execute("CREATE TABLE "+keyspace+".dianode ("
                    + "vid text,"
                    + "start text," + "end text,"
                    + "name list<frozen<attribute>>,"
                    + "color list<frozen<attribute>>,"
                    + "incoming_edges map<text, frozen<list<edge>>>,"
                    + "outgoing_edges map<text, frozen<list<edge>>>,"
                    + "PRIMARY KEY (vid, start, end)"
                    + ");");
            Thread.sleep(10000);
        }
        catch (InterruptedException ex)
        {
            Logger.getLogger(SingleTableModel.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public HashMap<String, ArrayList<String>> getAllAliveVertices(String first, String last) // Each key is the time instance and its value are the vIDs that are alive.
    {
        long tStart, tEnd, tDelta;
        
        HashMap<String, ArrayList<String>> vertices = new HashMap<String, ArrayList<String>>();
        vertices.put("allVertices", new ArrayList<String>()); // The "allVertices" key contains a list of all vIDs that are alive at some point in [first, last]
        
        tStart = System.currentTimeMillis();
        ResultSet rs = session.execute("SELECT vid, start, end FROM " + keyspace + ".dianode;");
        List<Row> rows = rs.all();
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for retrieving [start,end] info for all vertices: " + elapsedSeconds + " seconds, (Query Range: [" + first + ", " + last + "])");
        
        tStart = System.currentTimeMillis();
        for (Row row : rows)
        {
            int rowstart = Integer.parseInt(row.getString("start"));
            int rowend = Integer.parseInt(row.getString("end"));
            
            if (rowend <= Integer.parseInt(first) || Integer.parseInt(last) < rowstart) // Assumes correct intervals as input
                continue;
            
            String vid = row.getString("vid");
            int start = Math.max(Integer.parseInt(first), rowstart); // Only report values that are after both "first" and the diachronic node's "rowstart"
            int end = Math.min(Integer.parseInt(last), rowend); // Only report values that are before both "last" and the diachronic node's "rowend"
            for (int i = start; i <= end; i++)
            {
                if (!vertices.containsKey("" + i))
                    vertices.put("" + i, new ArrayList<String>());
                
                vertices.get("" + i).add(vid);
            }
            vertices.get("allVertices").add(vid);
        }
        List<String> duplicates = vertices.get("allVertices"); // Remove duplicate vIDs from "allVertices"
        Set<String> unique = new HashSet<String>(duplicates);
        vertices.put("allVertices", new ArrayList<String>(unique));
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for processing the info into a HashMap: " + elapsedSeconds + " seconds, (Query Range: [" + first + ", " + last + "])");
        return vertices;
    }
    public Map<String, Map<String, Edge>> getAllEdgesOfVertex(String vid, String timestamp) 
    {
        ResultSet rs = session.execute("SELECT incoming_edges, outgoing_edges FROM "+keyspace+".dianode " + "WHERE vid = '" + vid + "'");
        
        Row vertexRow = rs.one(); // We only expect one row
        
        Map<String, Map<String, Edge>> allEdges = new HashMap<String, Map<String, Edge>>();
        Map<String, Edge> incoming_edges_current = new HashMap<String, Edge>();
        Map<String, Edge> outgoing_edges_current = new HashMap<String, Edge>();
        
        TypeToken<List<UDTValue>> listOfEdges = new TypeToken<List<UDTValue>>(){};
        
        Map<String, List<UDTValue>> edgesUDTListIn = vertexRow.getMap("incoming_edges", TypeToken.of(String.class), listOfEdges);
        Map<String, List<Edge>> incoming_edges_diachronic = convertToEdgeList(edgesUDTListIn);

        for (String sourceID : incoming_edges_diachronic.keySet())
        {
            List<Edge> edges = incoming_edges_diachronic.get(sourceID);
            for (Edge edge : edges)
                if (Integer.parseInt(timestamp) >= Integer.parseInt(edge.start) && Integer.parseInt(timestamp) < Integer.parseInt(edge.end))
                {
                    incoming_edges_current.put(sourceID, edge);
                    break;
                }
        }
        
        Map<String, List<UDTValue>> edgesUDTListOut = vertexRow.getMap("outgoing_edges", TypeToken.of(String.class), listOfEdges);
        Map<String, List<Edge>> outgoing_edges_diachronic = convertToEdgeList(edgesUDTListOut); 
        
        for (String targetID : outgoing_edges_diachronic.keySet())
        {
            List<Edge> edges = outgoing_edges_diachronic.get(targetID);
            for (Edge edge : edges)
                if (Integer.parseInt(timestamp) >= Integer.parseInt(edge.start) && Integer.parseInt(timestamp) < Integer.parseInt(edge.end))
                {
                    outgoing_edges_current.put(targetID, edge);
                    break;
                }
        }
        
        allEdges.put("incoming_edges", incoming_edges_current);
        allEdges.put("outgoing_edges", outgoing_edges_current);
        
        return allEdges;
    }
    public List<DiaNode> getAllVerticesAndFilterAlive(String first, String last) // Each key is the time instance and its value are the vIDs that are alive.
    {
        long tStart, tEnd, tDelta;
        
        ArrayList<DiaNode> dianodes = new ArrayList<DiaNode>();
        
        tStart = System.currentTimeMillis();
        ResultSet rs = session.execute("SELECT * FROM " + keyspace + ".dianode;");
        List<Row> rows = rs.all();
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for retrieving all the diachronic nodes: " + elapsedSeconds + " seconds");
        
        tStart = System.currentTimeMillis();
        for (Row row : rows)
        {
            int rowstart = Integer.parseInt(row.getString("start"));
            int rowend = Integer.parseInt(row.getString("end"));
            
            if (rowend < Integer.parseInt(first) || Integer.parseInt(last) < rowstart) // Assumes correct intervals as input
                continue;
            
            String vid = row.getString("vid");
            int start = Math.max(Integer.parseInt(first), rowstart); // Only report values that are after both "first" and the diachronic node's "rowstart"
            int end = Math.min(Integer.parseInt(last), rowend); // Only report values that are before both "last" and the diachronic node's "rowend"
            
            DiaNode dn = new DiaNode(row);
            dn.keepValuesInInterval(first, last);
            dianodes.add(dn);
        }
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for filtering out the alive diachronic nodes: " + elapsedSeconds + " seconds, (Query Range: [" + first + ", " + last + "])");
        return dianodes;
    }

    @Override
    public List<SnapshotResult> getAvgVertexDegree(String first, String last) // Running example query range: [30,50]
    {
        long tStart, tEnd, tDelta;
        
        HashMap<String, Double> edgeCounts = new HashMap<String, Double>(); // Holds the total edge count at any point in the query range (e.g. [(30,2), (31,4), ..., (50,22)]
        HashMap<String, Double> vertexCounts = new HashMap<String, Double>(); // Holds the total vertex count at any point in the query range (e.g. [(30,4), (31,3), ..., (50,16)]
        
        HashMap<String, ArrayList<String>> vertices = getAllAliveVertices(first, last);
        ArrayList<String> allVertices = vertices.get("allVertices");
        
        for (int i=Integer.parseInt(first); i<=Integer.parseInt(last); i++)
        {
            ArrayList<String> instanceVIDs = vertices.get(""+i);
            if (instanceVIDs == null)
                vertexCounts.put(""+i, 0.0);
            else
                vertexCounts.put(""+i, (double) instanceVIDs.size());
        }
        
        PreparedStatement statement = session.prepare("SELECT start, end, outgoing_edges FROM "+keyspace+".dianode WHERE vid = ?");
        
        tStart = System.currentTimeMillis();
        List<ResultSetFuture> futures = new ArrayList<>();
        for (String vertex : allVertices)
        {
            ResultSetFuture resultSetFuture = session.executeAsync(statement.bind(vertex));
            futures.add(resultSetFuture);
        }
        
        ConcurrentLinkedQueue<Row> rows = new ConcurrentLinkedQueue<Row>();
        for (ResultSetFuture future : futures)
        {
            ResultSet rs = future.getUninterruptibly();
            Row row = rs.one();
            rows.add(row);
        }
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for retrieving the relevant alive nodes: " + elapsedSeconds + " seconds.");

        tStart = System.currentTimeMillis();
        for (Row row : rows) // For each diachronic node
        {
            String rowend = row.getString("end");
            if (Integer.parseInt(rowend) < Integer.parseInt(first)) // That means that the diachronic node's "start" and "end" time instances were BOTH before our query instance 
                continue;
            
            String rowstart = row.getString("start");
            
            int start = Math.max(Integer.parseInt(first), Integer.parseInt(rowstart)); // Only report values that are after both "first" and the diachronic node's "rowstart"
            int end = Math.min(Integer.parseInt(last), Integer.parseInt(rowend)); // Only report values that are before both "last" and the diachronic node's "rowend"
            
            TypeToken<List<UDTValue>> listOfEdges = new TypeToken<List<UDTValue>>() {};
            Map<String, List<UDTValue>> edgesUDTList = row.getMap("outgoing_edges", TypeToken.of(String.class), listOfEdges);
            Map<String, List<Edge>> outgoing_edges = SingleTableModel.convertToEdgeList(edgesUDTList); // Fetch all the edges of the diachronic node
            
            for (String targetID : outgoing_edges.keySet())
            {
                List<Edge> edges = outgoing_edges.get(targetID);
                for (Edge edge : edges) // For each edge in the diachronic node
                {   
                    int edgeStart = Math.max(start, Integer.parseInt(edge.start));
                    int edgeEnd = Math.min(end, Integer.parseInt(edge.end));
                    
                    for (int i=edgeStart; i<=edgeEnd; i++) // Increase the edge count for any edges found overlapping or intersecting the [start,end] range specified before
                    {
                        if (!edgeCounts.containsKey("" + i))
                            edgeCounts.put("" + i, 0.0);
                        edgeCounts.put("" + i, edgeCounts.get("" + i) + 1.0);
                    }
                }
            }
        }
        
        ArrayList<SnapshotResult> results = new ArrayList<SnapshotResult>();
        for (int i=Integer.parseInt(first); i<=Integer.parseInt(last); i++)
        {
            Double e_count = edgeCounts.get(""+i);
            Double v_count = vertexCounts.get(""+i);
            
            if (e_count == null || v_count == null || v_count == 0.0 || e_count == 0.0)
            {
                results.add(new SnapshotResult(""+i, 0.0));
                continue;
            }
            
            results.add(new SnapshotResult(""+i, (e_count / v_count)));
        }
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for processing and evaluating the AvgDeg query: " + elapsedSeconds + " seconds.");
        
        return results;
    }
    @Override
    public List<SnapshotResult> getAvgVertexDegreeFetchAllVertices(String first, String last)
    {
        long tStart, tEnd, tDelta;
        
        HashMap<String, Double> edgeCounts = new HashMap<String, Double>(); // Holds the total edge count at any point in the query range (e.g. [(30,2), (31,4), ..., (50,22)]
        HashMap<String, Double> vertexCounts = new HashMap<String, Double>(); // Holds the total vertex count at any point in the query range (e.g. [(30,4), (31,3), ..., (50,16)]
        
        List<DiaNode> dianodes = getAllVerticesAndFilterAlive(first, last);
        
        for (int i = Integer.parseInt(first); i <= Integer.parseInt(last); i++)
            vertexCounts.put(""+i, 0.0);
        
        tStart = System.currentTimeMillis();
        for (DiaNode dn : dianodes) // For each diachronic node
        {
            for (int i = Integer.parseInt(dn.getStart()); i <= Integer.parseInt(dn.getEnd()); i++)
                vertexCounts.put(""+i, vertexCounts.get(""+i) + 1.0);
            
            Map<String, List<Edge>> outgoing_edges = dn.getOutgoing_edges();
            
            for (String targetID : outgoing_edges.keySet())
            {
                List<Edge> edges = outgoing_edges.get(targetID);
                for (Edge edge : edges) // For each edge in the diachronic node
                {
                    int edgeStart = Integer.parseInt(edge.start);
                    int edgeEnd = Integer.parseInt(edge.end);
                    
                    for (int i = edgeStart; i <= edgeEnd; i++) // Increase the edge count for any edges found overlapping or intersecting the [start,end] range specified before
                    {
                        if (!edgeCounts.containsKey("" + i))
                            edgeCounts.put("" + i, 0.0);
                        
                        edgeCounts.put("" + i, edgeCounts.get("" + i) + 1.0);
                    }
                }
            }
        }
        
        ArrayList<SnapshotResult> results = new ArrayList<SnapshotResult>();
        for (int i = Integer.parseInt(first); i <= Integer.parseInt(last); i++)
        {
            Double e_count = edgeCounts.get("" + i);
            Double v_count = vertexCounts.get("" + i);
            
            if (e_count == null || v_count == null || v_count == 0.0 || e_count == 0.0)
            {
                results.add(new SnapshotResult("" + i, 0.0));
                continue;
            }
            
            results.add(new SnapshotResult("" + i, (e_count / v_count)));
        }
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for processing and evaluating the AvgDeg query: " + elapsedSeconds + " seconds.");
        
        return results;
    }
    
    @Override
    public HashMap<String, HashMap<String, Integer>> getDegreeDistribution(String first, String last)
    {
        long tStart, tEnd, tDelta;
        
        HashMap<String, List<Double>> allDegreesPerTimeInstance = new HashMap<String, List<Double>>(); // Holds the total degree count for all vertices that exist in each time instance (e.g. [(0,[2,2,5,1,3,2]), (1,[5,2,1,1,2]), ...])

        HashMap<String, ArrayList<String>> vertices = getAllAliveVertices(first, last);
        ArrayList<String> allVertices = vertices.get("allVertices");      
        
        PreparedStatement statement = session.prepare("SELECT start, end, outgoing_edges FROM "+keyspace+".dianode WHERE vid = ?");

        tStart = System.currentTimeMillis();
        List<ResultSetFuture> futures = new ArrayList<>();
        for (String vertex : allVertices)
        {
            ResultSetFuture resultSetFuture = session.executeAsync(statement.bind(vertex));
            futures.add(resultSetFuture);
        }

        ConcurrentLinkedQueue<Row> rows = new ConcurrentLinkedQueue<Row>();
        for (ResultSetFuture future : futures)
        {
            ResultSet rs = future.getUninterruptibly();
            Row row = rs.one();
            rows.add(row);
        }
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for retrieving the relevant alive nodes: " + elapsedSeconds + " seconds.");
        
        tStart = System.currentTimeMillis();
        for (Row row : rows) // For each diachronic node
        {
            String rowend = row.getString("end");
            if (Integer.parseInt(rowend) < Integer.parseInt(first)) // That means that the diachronic node's "start" and "end" time instances were BOTH before our query instance
                continue;

            String rowstart = row.getString("start");

            int start = Math.max(Integer.parseInt(first), Integer.parseInt(rowstart)); // Only report values that are after both "first" and the diachronic node's "rowstart"
            int end = Math.min(Integer.parseInt(last), Integer.parseInt(rowend)); // Only report values that are before both "last" and the diachronic node's "rowend"

            TypeToken<List<UDTValue>> listOfEdges = new TypeToken<List<UDTValue>>() {};
            Map<String, List<UDTValue>> edgesUDTList = row.getMap("outgoing_edges", TypeToken.of(String.class), listOfEdges);
            Map<String, List<Edge>> outgoing_edges = SingleTableModel.convertToEdgeList(edgesUDTList); // Fetch all the edges of the diachronic node
            
            HashMap<String, Double> vertexDegreePerTimeInstance = new HashMap<String, Double>(); // Holds all the degrees for this particular vertex for all query instances that it exists in
            
            for (int i = start; i <= end; i++) // The vertex has "0" degree in all time instances contained in the query range
                vertexDegreePerTimeInstance.put(""+i, 0.0);

            for (String targetID : outgoing_edges.keySet())
            {
                List<Edge> edges = outgoing_edges.get(targetID);
                for (Edge edge : edges) // For each edge in the diachronic node
                {
                    int edgeStart = Math.max(start, Integer.parseInt(edge.start));
                    int edgeEnd = Math.min(end, Integer.parseInt(edge.end));

                    for (int i = edgeStart; i <= edgeEnd; i++) // Increase the edge count for any edges found overlapping or intersecting the [start,end] range specified before
                        vertexDegreePerTimeInstance.put(""+i, vertexDegreePerTimeInstance.get(""+i) + 1.0);
                }
            }
            
            for (String instance : vertexDegreePerTimeInstance.keySet()) // For each time instance in this vertex's history add the vertex's degree to the corresponding overall degree list for that instance, found in "allDegreesPerTimeInstance"
            {
                List<Double> instanceDegrees = allDegreesPerTimeInstance.get(instance);
                
                if (instanceDegrees == null)
                    instanceDegrees = new ArrayList<Double>();
                instanceDegrees.add(vertexDegreePerTimeInstance.get(instance));
                allDegreesPerTimeInstance.put(instance, instanceDegrees);
            }
        }
        
        HashMap<String, HashMap<String, Integer>> results = new HashMap<String, HashMap<String, Integer>>();
        for (String instance : allDegreesPerTimeInstance.keySet())
        {
            List<Double> degrees = allDegreesPerTimeInstance.get(instance);
            
            HashMap<String, Integer> degreeDistr = new HashMap<String, Integer>();
            for (Double degree : degrees)
            {
                Integer count = degreeDistr.get(degree.toString());
                degreeDistr.put(degree.toString(), (count == null) ? 1 : count + 1);
            }
            results.put(instance, degreeDistr);
        }
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for processing and evaluating the DegDistr query: " + elapsedSeconds + " seconds.");
        return results;
    }
    @Override
    public HashMap<String, HashMap<String, Integer>> getDegreeDistributionFetchAllVertices(String first, String last)
    {
        long tStart, tEnd, tDelta;
        
        HashMap<String, List<Double>> allDegreesPerTimeInstance = new HashMap<String, List<Double>>(); // Holds the total degree count for all vertices that exist in each time instance (e.g. [(0,[2,2,5,1,3,2]), (1,[5,2,1,1,2]), ...])
        
        List<DiaNode> dianodes = getAllVerticesAndFilterAlive(first, last);
        
        tStart = System.currentTimeMillis();
        for (DiaNode dn : dianodes) // For each diachronic node
        {
            String rowend = dn.getEnd();
            String rowstart = dn.getStart();
            
            int start = Integer.parseInt(rowstart);
            int end = Integer.parseInt(rowend);
            
            Map<String, List<Edge>> outgoing_edges = dn.getOutgoing_edges();
            
            HashMap<String, Double> vertexDegreePerTimeInstance = new HashMap<String, Double>(); // Holds all the degrees for this particular vertex for all query instances that it exists in
            
            for (int i = start; i <= end; i++) // The vertex has "0" degree in all time instances contained in the query range
                vertexDegreePerTimeInstance.put("" + i, 0.0);
            
            for (String targetID : outgoing_edges.keySet())
            {
                List<Edge> edges = outgoing_edges.get(targetID);
                for (Edge edge : edges) // For each edge in the diachronic node
                {
                    int edgeStart = Math.max(start, Integer.parseInt(edge.start));
                    int edgeEnd = Math.min(end, Integer.parseInt(edge.end));
                    
                    for (int i = edgeStart; i <= edgeEnd; i++) // Increase the edge count for any edges found overlapping or intersecting the [start,end] range specified before
                        vertexDegreePerTimeInstance.put("" + i, vertexDegreePerTimeInstance.get("" + i) + 1.0);
                }
            }
            
            for (String instance : vertexDegreePerTimeInstance.keySet()) // For each time instance in this vertex's history add the vertex's degree to the corresponding overall degree list for that instance, found in "allDegreesPerTimeInstance"
            {
                List<Double> instanceDegrees = allDegreesPerTimeInstance.get(instance);
                
                if (instanceDegrees == null)
                    instanceDegrees = new ArrayList<Double>();
                
                instanceDegrees.add(vertexDegreePerTimeInstance.get(instance));
                allDegreesPerTimeInstance.put(instance, instanceDegrees);
            }
        }
        
        HashMap<String, HashMap<String, Integer>> results = new HashMap<String, HashMap<String, Integer>>();
        for (String instance : allDegreesPerTimeInstance.keySet())
        {
            List<Double> degrees = allDegreesPerTimeInstance.get(instance);
            
            HashMap<String, Integer> degreeDistr = new HashMap<String, Integer>();
            for (Double degree : degrees)
            {
                Integer count = degreeDistr.get(degree.toString());
                degreeDistr.put(degree.toString(), (count == null) ? 1 : count + 1);
            }
            results.put(instance, degreeDistr);
        }
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for processing and evaluating the DegDistrAll query: " + elapsedSeconds + " seconds.");
        return results;
    }
    @Override
    public List<String> getOneHopNeighborhood(String vid, String first, String last)
    {
        long tStart, tEnd, tDelta;
        List<String> results = new ArrayList<String>();
        
        tStart = System.currentTimeMillis();
        ResultSet rs = session.execute("SELECT outgoing_edges FROM "+keyspace+".dianode " //We make the assumption that "vid" exists at "timestamp", i.e. vid.start <= timestamp < vid.end
                + "WHERE vid = '" + vid + "'");
        
        Row row = rs.one(); // We only expect one row
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for retrieving the outgoing edges of a diachronic node: " + elapsedSeconds + " seconds, (OneHop on VID, Timestamps: [" + vid + ", " + first + " to " + last + "])");
        if (row == null)
            return Collections.EMPTY_LIST;
        
        tStart = System.currentTimeMillis();
        TypeToken<List<UDTValue>> listOfEdges = new TypeToken<List<UDTValue>>() {};
        Map<String, List<UDTValue>> edgesUDTList = row.getMap("outgoing_edges", TypeToken.of(String.class), listOfEdges);
        Map<String, List<Edge>> outgoing_edges = SingleTableModel.convertToEdgeList(edgesUDTList);
        
        for (String targetID : outgoing_edges.keySet())
        {
            List<Edge> edges = outgoing_edges.get(targetID);
            for (Edge edge : edges)
                if ((Integer.parseInt(first) >= Integer.parseInt(edge.start) && Integer.parseInt(first) < Integer.parseInt(edge.end)) ||
                        (Integer.parseInt(last) >= Integer.parseInt(edge.start) && Integer.parseInt(last) < Integer.parseInt(edge.end)))
                {
                    results.add(targetID);
                    break;
                }
        }
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for extracting the neighbors of a vertex: " + elapsedSeconds + " seconds, (OneHop on VID, Timestamps: [" + vid + ", " + first + " to " + last + "])");
        return results;
    }

    @Override
    public DiaNode getVertexHistory(String vid, String first, String last) 
    {
        ResultSet rs = session.execute("SELECT * FROM "+keyspace+".dianode "
                + "WHERE vid = '" + vid + "'");
        
        Row row = rs.one(); // We only expect one row
        
        if (row == null)
            return new DiaNode(vid);
        
        DiaNode dn = new DiaNode(row);   
        dn.keepValuesInInterval(first, last);
        
        return dn;
    }
    
    public Vertex getVertexInstance(String vid, String timestamp)
    {
        ResultSet rs = session.execute("SELECT * FROM "+keyspace+".dianode "
                + "WHERE vid = '" + vid + "'");
        
        Row row = rs.one(); // We only expect one row
        
        DiaNode dn = new DiaNode(row);   
        Vertex ver = dn.convertToVertex(timestamp);
        
        return ver;
        
    }
    public void insert(DiaNode ver)
    {
        
        ResultSet rs = session.execute("SELECT * FROM "+keyspace+".dianode "
                + "WHERE vid = '" + ver.getVid() + "'");
        
        List<Row> rows = rs.all();
        if (rows.isEmpty())
        {
            int i = 0;
            String[] attributesStringArray = new String[2];
            for (String attrName : ver.getAttributes().keySet())
            {
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for (Interval interval : ver.getAttributes().get(attrName))
                {
                    sb.append(interval.toString());
                    sb.append(",");
                }
                String temp = sb.substring(0, sb.length()-1);
                temp += "]";
                attributesStringArray[i] = temp;
                i++;
            }
            session.execute("INSERT INTO "+keyspace+".dianode (vid, start, end, name, color)"
                    + "VALUES ('" + ver.getVid() + "', '" + ver.getStart() + "', '" + ver.getEnd() + "', "
                    + attributesStringArray[1] + ", "
                    + attributesStringArray[0]
                    + ");");
        }
    }

    @Override
    public void insertEdge(String sourceID, String targetID, String start, String end, String label, String weight)
    {
        // First, insert the edge as an outgoing edge
        ResultSet rs = session.execute("SELECT * FROM "+keyspace+".dianode "
                + "WHERE vid = '" + sourceID + "'");

        Row vertexRow = rs.one(); // We only expect one row
        TypeToken<List<UDTValue>> listOfEdges = new TypeToken<List<UDTValue>>() {};
        Map<String, List<UDTValue>> edgesUDTList = vertexRow.getMap("outgoing_edges", TypeToken.of(String.class), listOfEdges);
        Map<String, List<Edge>> edges = convertToEdgeList(edgesUDTList);
        
        if (edges.isEmpty())
        {
            session.execute("UPDATE "+keyspace+".dianode SET outgoing_edges = {'" 
                    + targetID + "' : [" + new Edge(label, weight, targetID, start, end) 
                    + "] } WHERE vid = '" + sourceID + "' AND start = '" + vertexRow.getString("start") + "' AND end = '" + vertexRow.getString("end") + "';");
        }
        else
        {
            List<Edge> vertex_edges;
            if (edges.get(targetID) != null)
            {
                vertex_edges = edges.get(targetID);
                Edge lastEdge = vertex_edges.get(vertex_edges.size()-1); 
                lastEdge.end = start;
            }
            else
                vertex_edges = new ArrayList<Edge>();
            
            Edge newEdge = new Edge(label, weight, targetID, start, end);
            vertex_edges.add(newEdge);
            edges.put(targetID, vertex_edges);
            
            String allEdges = vertex_edges.get(0).toString();
            for (Edge edge : vertex_edges.subList(1,vertex_edges.size()))
            {
                allEdges = allEdges.concat(",");
                allEdges = allEdges.concat(edge.toString());
            }
            session.execute("UPDATE "+keyspace+".dianode SET outgoing_edges = outgoing_edges + {'"
                    + targetID + "' : [" + allEdges
                    + "] } WHERE vid = '" + sourceID + "' AND start = '" + vertexRow.getString("start") + "' AND end = '" + vertexRow.getString("end") + "';");
        }
        
        // Then, insert the edge as an incoming edge
        rs = session.execute("SELECT * FROM "+keyspace+".dianode "
                + "WHERE vid = '" + targetID + "'");
        
        vertexRow = rs.one(); // We only expect one row
        listOfEdges = new TypeToken<List<UDTValue>>(){};
        if (vertexRow != null)
            edgesUDTList = vertexRow.getMap("incoming_edges", TypeToken.of(String.class), listOfEdges);
        else
            edgesUDTList = null;
        edges = convertToEdgeList(edgesUDTList);
        
        if (edges.isEmpty())
        {
            session.execute("UPDATE "+keyspace+".dianode SET incoming_edges = {'"
                    + sourceID + "' : [" + new Edge(label, weight, sourceID, start, end)
                    + "] } WHERE vid = '" + targetID + "' AND start = '" + vertexRow.getString("start") + "' AND end = '" + vertexRow.getString("end") + "';");
        }
        else
        {
            List<Edge> vertex_edges;
            if (edges.get(sourceID) != null)
            {
                vertex_edges = edges.get(sourceID);
                Edge lastEdge = vertex_edges.get(vertex_edges.size() - 1);
                lastEdge.end = start;
            }
            else
                vertex_edges = new ArrayList<Edge>();
            
            Edge newEdge = new Edge(label, weight, sourceID, start, end);
            vertex_edges.add(newEdge);
            edges.put(sourceID, vertex_edges);

            String allEdges = vertex_edges.get(0).toString();
            for (Edge edge : vertex_edges.subList(1, vertex_edges.size()))
            {
                allEdges = allEdges.concat(",");
                allEdges = allEdges.concat(edge.toString());
            }
            session.execute("UPDATE "+keyspace+".dianode SET incoming_edges = incoming_edges + {'"
                    + sourceID + "' : [" + allEdges
                    + "] } WHERE vid = '" + targetID + "' AND start = '" + vertexRow.getString("start") + "' AND end = '" + vertexRow.getString("end") + "';");
        }
    }

    @Override
    public void insertVertex(String vid, String name, String start, String end, String color) 
    {      
        DiaNode ver = new DiaNode(vid, start, end);
        List<Interval> namesList = new ArrayList<Interval>();
        namesList.add(new Interval(name,start,end));
        
        List<Interval> colorsList = new ArrayList<Interval>();
        colorsList.add(new Interval(color,start,end));
        
        ver.insertAttribute("name", namesList);
        ver.insertAttribute("color", colorsList);
        
        insert(ver);
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


                session.execute("INSERT INTO "+keyspace+".dianode (vid, start, end, name, color, incoming_edges, outgoing_edges)" //Assume only one edge per vertex pair in graph 0
                        + "VALUES ('" + ver.getVid() + "', '" + start + "', '" + end + "', "
                                + "[{value: '" + name + "', start: '00000000', end: '" + DataModel.padWithZeros(""+snap_count) + "'}], "
                                        + "[{value: '" + color + "', start: '00000000', end: '" + DataModel.padWithZeros(""+snap_count) + "'}], "
                                                + "{" + allIncEdgesstr + "}, "
                                                        + "{" + allOutEdgesstr + "}"
                                                                + ");");
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
                    updateVertexAttribute(verID, attrName, value, curVersion);
                }
            }
            file.close();
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
        ResultSet rs = session.execute("SELECT * FROM "+keyspace+".dianode " + "WHERE vid = '" + vid + "'");
        
        Row vertexRow = rs.one(); // We only expect one row
        List<UDTValue> attrUDTList = vertexRow.getList(attrName, UDTValue.class);
        List<Interval> attrList = convertToIntervals(attrUDTList);
        
        Interval toBeDeleted = attrList.get(attrList.size()-1);
        
        // Remove the old interval
        session.execute("UPDATE "+keyspace+".dianode SET " + attrName + " = " + attrName + " - ["
                + toBeDeleted
                + "] WHERE vid = '" + vid + "' AND start = '" + vertexRow.getString("start") + "' AND end = '" + vertexRow.getString("end") + "';");
        
        // Add the updated old interval and the new interval
        session.execute("UPDATE "+keyspace+".dianode SET " + attrName + " = " + attrName + " + ["
                + new Interval(toBeDeleted.value, toBeDeleted.start, timestamp)
                + "] WHERE vid = '" + vid + "' AND start = '" + vertexRow.getString("start") + "' AND end = '" + vertexRow.getString("end") + "';");
        
        session.execute("UPDATE "+keyspace+".dianode SET " + attrName + " = " + attrName + " + ["
                + new Interval(attrValue,timestamp,"Infinity")
                + "] WHERE vid = '" + vid + "' AND start = '" + vertexRow.getString("start") + "' AND end = '" + vertexRow.getString("end") + "';");
    }

    @Override
    public void useKeyspace() 
    {
        this.session.execute("USE "+keyspace+";");
    }

}

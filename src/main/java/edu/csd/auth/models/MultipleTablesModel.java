package edu.csd.auth.models;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
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
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MultipleTablesModel implements DataModel
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
        } catch (CharacterCodingException e)
        {
            e.printStackTrace();
            return "";
        }
        return data;
    }

    public MultipleTablesModel(Session session, String keyspace)
    {
        this.session = session;
        this.keyspace = keyspace;
    }

    @Override
    public void createSchema()
    {
        session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";");

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':1};");

        session.execute("USE " + keyspace + ";");

        session.execute("CREATE TABLE " + keyspace + ".vertex (" + "vid text,"
                + "start text," + "end text,"
                + "PRIMARY KEY (vid, start, end)"
                + ");");
        session.execute("CREATE TABLE " + keyspace + ".vertex_name (" + "vid text,"
                + "name text," + "timestamp text, "
                + "PRIMARY KEY (vid, timestamp)"
                + ") WITH CLUSTERING ORDER BY (timestamp DESC);");
        session.execute("CREATE TABLE " + keyspace + ".vertex_color (" + "vid text,"
                + "color text," + "timestamp text, "
                + "PRIMARY KEY (vid, timestamp)"
                + ") WITH CLUSTERING ORDER BY (timestamp DESC);");
        session.execute("CREATE TABLE " + keyspace + ".edge_outgoing ("
                + "start text," + "end text,"
                + "sourceID text," + "targetID text,"
                //                + "PRIMARY KEY (sourceID, targetID, start, end)"
                + "PRIMARY KEY (sourceID, start, end, targetID)"
                + ");");
        session.execute("CREATE TABLE " + keyspace + ".edge_label_outgoing ("
                + "label text," + "timestamp text, "
                + "sourceID text," + "targetID text,"
                + "PRIMARY KEY (sourceID, timestamp, targetID)"
                + ") WITH CLUSTERING ORDER BY (timestamp DESC, targetID DESC);");
        session.execute("CREATE TABLE " + keyspace + ".edge_weight_outgoing ("
                + "weight text," + "timestamp text, "
                + "sourceID text," + "targetID text,"
                + "PRIMARY KEY (sourceID, timestamp, targetID)"
                + ") WITH CLUSTERING ORDER BY (timestamp DESC, targetID DESC);");
        session.execute("CREATE TABLE " + keyspace + ".edge_incoming ("
                + "start text," + "end text,"
                + "sourceID text," + "targetID text,"
                //                + "PRIMARY KEY (targetID, sourceID, start, end)"
                + "PRIMARY KEY (targetID, start, end, sourceID)"
                + ");");
        session.execute("CREATE TABLE " + keyspace + ".edge_label_incoming ("
                + "label text," + "timestamp text, "
                + "sourceID text," + "targetID text,"
                + "PRIMARY KEY (targetID, timestamp, sourceID)"
                + ") WITH CLUSTERING ORDER BY (timestamp DESC, sourceID DESC);");
        session.execute("CREATE TABLE " + keyspace + ".edge_weight_incoming ("
                + "weight text," + "timestamp text, "
                + "sourceID text," + "targetID text,"
                + "PRIMARY KEY (targetID, timestamp, sourceID)"
                + ") WITH CLUSTERING ORDER BY (timestamp DESC, sourceID DESC);");
    }

    public HashMap<String, ArrayList<String>> getAllAliveVertices(String first, String last) // Each key is the time instance and its value are the vIDs that are alive.
    {
        long tStart, tEnd, tDelta;

        HashMap<String, ArrayList<String>> vertices = new HashMap<String, ArrayList<String>>();
        vertices.put("allVertices", new ArrayList<String>()); // The "allVertices" key contains a list of all vIDs that are alive at some point in [first, last]

        tStart = System.currentTimeMillis();
        ResultSet rs = session.execute("SELECT * FROM " + keyspace + ".vertex;");
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

            if (rowend < Integer.parseInt(first) || Integer.parseInt(last) < rowstart) // Assumes correct intervals as input
            {
                continue;
            }

            String vid = row.getString("vid");
            int start = Math.max(Integer.parseInt(first), rowstart); // Only report values that are after both "first" and the diachronic node's "rowstart"
            int end = Math.min(Integer.parseInt(last), rowend); // Only report values that are before both "last" and the diachronic node's "rowend"
            for (int i = start; i <= end; i++)
            {
                if (!vertices.containsKey("" + i))
                {
                    vertices.put("" + i, new ArrayList<String>());
                }

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

    private ConcurrentLinkedQueue<Row> getAllEdgesAndFilterAlive(String first, String last)
    {
        long tStart = System.currentTimeMillis();
        ResultSet result = session.execute("SELECT * FROM " + keyspace + ".edge_outgoing;");

        ConcurrentLinkedQueue<Row> rows = new ConcurrentLinkedQueue<Row>();
        while (!result.isExhausted())
        {
            Row row = result.one();
            String rowstart = row.getString("start");
            String rowend = row.getString("end");

            if (Integer.parseInt(rowend) < Integer.parseInt(first) || Integer.parseInt(last) < Integer.parseInt(rowstart))
            {
                continue;
            }

            rows.add(row);
        }
        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for retrieving all outgoing edges of all nodes: " + elapsedSeconds + " seconds.");

        return rows;
    }

    public Map<String, Map<String, Edge>> getAllEdgesOfVertex(String vid, String timestamp)
    {
        // First retrieve all labels and weights for all the outgoing and incoming vertices (found on the otherEnd of the edges) of "vid"
        Map<String, TreeMap<String, String>> all_outgoing_labels = getAttributeOfDirectedEdge(vid, timestamp, "label", "outgoing"); // [target, [timestamp, label]]
        Map<String, TreeMap<String, String>> all_outgoing_weights = getAttributeOfDirectedEdge(vid, timestamp, "weight", "outgoing"); // [target, [timestamp, weight]]
        Map<String, TreeMap<String, String>> all_incoming_labels = getAttributeOfDirectedEdge(vid, timestamp, "label", "incoming"); // [source, [timestamp, label]]
        Map<String, TreeMap<String, String>> all_incoming_weights = getAttributeOfDirectedEdge(vid, timestamp, "weight", "incoming"); // [source, [timestamp, weight]]

        Map<String, Map<String, Edge>> allEdges = new HashMap<String, Map<String, Edge>>();

        // First retrieve the outgoing edges of the vertex
        ResultSet rs = session.execute("SELECT * FROM " + keyspace + ".edge_outgoing " + "WHERE sourceID = '" + vid + "' " + "AND start <= '" + timestamp + "'");

        List<Row> rows = rs.all();

        Map<String, Edge> outgoing_edges = new HashMap<String, Edge>();

        for (Row row : rows)
        {
            String end = row.getString("end");
            if (Integer.parseInt(timestamp) > Integer.parseInt(end))
            {
                continue;
            }
            String start = row.getString("start");
            String targetID = row.getString("targetID");
            TreeMap<String, String> out_labels = all_outgoing_labels.get(targetID);
            String label = getLastValue(out_labels, timestamp);
            TreeMap<String, String> out_weights = all_outgoing_weights.get(targetID);
            String weight = getLastValue(out_weights, timestamp);

            Edge newedge = new Edge(label, weight, targetID, start, end);
            outgoing_edges.put(targetID, newedge);
        }

        // Then retrieve the incoming edges of the vertex
        rs = session.execute("SELECT * FROM " + keyspace + ".edge_incoming " + "WHERE targetID = '" + vid + "' " + "AND start <= '" + timestamp + "'");

        rows = rs.all();

        Map<String, Edge> incoming_edges = new HashMap<String, Edge>();

        for (Row row : rows)
        {
            String end = row.getString("end");
            if (Integer.parseInt(timestamp) > Integer.parseInt(end))
            {
                continue;
            }
            String start = row.getString("start");
            String sourceID = row.getString("sourceID");
            TreeMap<String, String> in_labels = all_incoming_labels.get(sourceID);
            String label = getLastValue(in_labels, timestamp);
            TreeMap<String, String> in_weights = all_incoming_weights.get(sourceID);
            String weight = getLastValue(in_weights, timestamp);

            Edge newedge = new Edge(label, weight, sourceID, start, end);
            incoming_edges.put(sourceID, newedge);
        }

        allEdges.put("outgoing_edges", outgoing_edges);
        allEdges.put("incoming_edges", incoming_edges);

        return allEdges;
    }

    private Map<String, TreeMap<String, String>> getAttributeOfDirectedEdge(String vid, String timestamp, String attribute, String direction)
    {
        String typeID;
        if (direction.equals("outgoing"))
        {
            typeID = "sourceID";
        } else
        {
            typeID = "targetID";
        }

        ResultSet rs = session.execute("SELECT * FROM " + keyspace + ".edge_" + attribute + "_" + direction + " " + "WHERE " + typeID + " = '" + vid + "' " + "AND timestamp <= '" + timestamp + "'");

        Map<String, TreeMap<String, String>> all_attribute = new HashMap<String, TreeMap<String, String>>(); // [targetID, [timestamp,label] ]

        List<Row> rows = rs.all();

        for (Row row : rows)
        {
            String label = row.getString(attribute);
            String rowtimestamp = row.getString("timestamp");
            if (direction.equals("outgoing")) //Reversed because...
            {
                typeID = "targetID";
            } else
            {
                typeID = "sourceID";
            }
            String ID = row.getString(typeID);

            if (!all_attribute.containsKey(ID))
            {
                TreeMap<String, String> temp = new TreeMap<String, String>();
                all_attribute.put(ID, temp);
            }
            TreeMap<String, String> changes = all_attribute.get(ID);
            changes.put(rowtimestamp, label);
            all_attribute.put(ID, changes);
        }

        return all_attribute;
    }

    @Override
    public List<SnapshotResult> getAvgVertexDegree(String first, String last)
    {
        long tStart, tEnd, tDelta;

        HashMap<String, Double> edgeCounts = new HashMap<String, Double>(); // Holds the total edge count at any point in the query range (e.g. [(30,2), (31,4), ..., (50,22)]
        HashMap<String, Double> vertexCounts = new HashMap<String, Double>(); // Holds the total vertex count at any point in the query range (e.g. [(30,4), (31,3), ..., (50,16)]

        HashMap<String, ArrayList<String>> vertices = getAllAliveVertices(first, last);
        ArrayList<String> allVertices = vertices.get("allVertices");

        for (int i = Integer.parseInt(first); i <= Integer.parseInt(last); i++)
        {
            ArrayList<String> instanceVIDs = vertices.get("" + i);
            if (instanceVIDs == null)
            {
                vertexCounts.put("" + i, 0.0);
            } else
            {
                vertexCounts.put("" + i, (double) instanceVIDs.size());
            }
        }

        PreparedStatement statement = session.prepare("SELECT start, end, sourceID FROM " + keyspace + ".edge_outgoing WHERE sourceID = ?");

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
            List<Row> allrows = rs.all();
            rows.addAll(allrows);
        }
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for retrieving the edges from the relevant alive nodes: " + elapsedSeconds + " seconds.");

        tStart = System.currentTimeMillis();
        for (Row row : rows)
        {
            String rowend = row.getString("end");
            if (Integer.parseInt(rowend) < Integer.parseInt(first)) // That means that the diachronic node's "start" and "end" time instances were BOTH before our query instance
            {
                continue;
            }

            String rowstart = row.getString("start");
            int start = Math.max(Integer.parseInt(first), Integer.parseInt(rowstart)); // Only report values that are after both "first" and the diachronic node's "rowstart"
            int end = Math.min(Integer.parseInt(last), Integer.parseInt(rowend)); // Only report values that are before both "last" and the diachronic node's "rowend"
            for (int i = start; i <= end; i++) // Increase the edge count for any edges found overlapping or intersecting the [start,end] range specified before
            {
                if (!edgeCounts.containsKey("" + i))
                {
                    edgeCounts.put("" + i, 0.0);
                }
                edgeCounts.put("" + i, edgeCounts.get("" + i) + 1.0);
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

        HashMap<String, ArrayList<String>> vertices = getAllAliveVertices(first, last);

        for (int i = Integer.parseInt(first); i <= Integer.parseInt(last); i++)
        {
            ArrayList<String> instanceVIDs = vertices.get("" + i);
            if (instanceVIDs == null)
            {
                vertexCounts.put("" + i, 0.0);
            } else
            {
                vertexCounts.put("" + i, (double) instanceVIDs.size());
            }
        }

        ConcurrentLinkedQueue<Row> rows = getAllEdgesAndFilterAlive(first, last);

        tStart = System.currentTimeMillis();
        for (Row row : rows)
        {
            String rowend = row.getString("end");
            String rowstart = row.getString("start");

            int start = Math.max(Integer.parseInt(first), Integer.parseInt(rowstart)); // Only report values that are after both "first" and the diachronic node's "rowstart"
            int end = Math.min(Integer.parseInt(last), Integer.parseInt(rowend)); // Only report values that are before both "last" and the diachronic node's "rowend"
            for (int i = start; i <= end; i++) // Increase the edge count for any edges found overlapping or intersecting the [start,end] range specified before
            {
                if (!edgeCounts.containsKey("" + i))
                {
                    edgeCounts.put("" + i, 0.0);
                }

                edgeCounts.put("" + i, edgeCounts.get("" + i) + 1.0);
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
        System.out.println("Time required for processing and evaluating the AvgDegAll query: " + elapsedSeconds + " seconds.");
        return results;
    }

    public List<Vertex> getBatchVertexInstances(Set<Integer> vids, String timestamp)
    {
        PreparedStatement statement_name = session.prepare("SELECT * FROM " + keyspace + ".vertex_name WHERE vid = ? AND timestamp <= ? LIMIT 1");
        PreparedStatement statement_color = session.prepare("SELECT * FROM " + keyspace + ".vertex_color WHERE vid = ? AND timestamp <= ? LIMIT 1");

        PreparedStatement statement_out_label = session.prepare("SELECT * FROM " + keyspace + ".edge_label_outgoing WHERE sourceID = ? AND timestamp <= ?");
        PreparedStatement statement_out_weight = session.prepare("SELECT * FROM " + keyspace + ".edge_weight_outgoing WHERE sourceID = ? AND timestamp <= ?");
        PreparedStatement statement_in_label = session.prepare("SELECT * FROM " + keyspace + ".edge_label_incoming WHERE targetID = ? AND timestamp <= ?");
        PreparedStatement statement_in_weight = session.prepare("SELECT * FROM " + keyspace + ".edge_weight_incoming WHERE targetID = ? AND timestamp <= ?");

        PreparedStatement statement_out = session.prepare("SELECT * FROM " + keyspace + ".edge_outgoing WHERE sourceID = ? AND start <= ?");
        PreparedStatement statement_in = session.prepare("SELECT * FROM " + keyspace + ".edge_incoming WHERE targetID = ? AND start <= ?");

        long tStart = System.currentTimeMillis();

        final ExecutorService ceName = Executors.newSingleThreadExecutor();
        final ExecutorService ceColor = Executors.newSingleThreadExecutor();
        final ExecutorService ceOutLabel = Executors.newSingleThreadExecutor();
        final ExecutorService ceOutWeight = Executors.newSingleThreadExecutor();
        final ExecutorService ceInLabel = Executors.newSingleThreadExecutor();
        final ExecutorService ceInWeight = Executors.newSingleThreadExecutor();
        final ExecutorService ceOut = Executors.newSingleThreadExecutor();
        final ExecutorService ceIn = Executors.newSingleThreadExecutor();

        final CountDownLatch doneSignal = new CountDownLatch(vids.size() * 8); // The count of tables we are querying is 8

        ConcurrentHashMap<String, String> map_vertex_name = new ConcurrentHashMap<String, String>();
        ConcurrentHashMap<String, String> map_vertex_color = new ConcurrentHashMap<String, String>();

        ConcurrentHashMap<String, Map<String, TreeMap<String, String>>> map_edge_out_label = new ConcurrentHashMap<String, Map<String, TreeMap<String, String>>>();
        ConcurrentHashMap<String, Map<String, TreeMap<String, String>>> map_edge_out_weight = new ConcurrentHashMap<String, Map<String, TreeMap<String, String>>>();
        ConcurrentHashMap<String, Map<String, TreeMap<String, String>>> map_edge_in_label = new ConcurrentHashMap<String, Map<String, TreeMap<String, String>>>();
        ConcurrentHashMap<String, Map<String, TreeMap<String, String>>> map_edge_in_weight = new ConcurrentHashMap<String, Map<String, TreeMap<String, String>>>();

        ConcurrentHashMap<String, ResultSet> map_edge_out = new ConcurrentHashMap<String, ResultSet>();
        ConcurrentHashMap<String, ResultSet> map_edge_in = new ConcurrentHashMap<String, ResultSet>();

        List<Vertex> results = new ArrayList<>();

        for (Integer i : vids)
        {
            String s_i = Integer.toString(i);

            ResultSetFuture resultSetFuture_name = session.executeAsync(statement_name.bind(s_i, timestamp));
            Futures.addCallback(resultSetFuture_name,
                    new FutureCallback<ResultSet>()
            {
                @Override
                public void onSuccess(ResultSet result)
                {
                    Row row = result.one();
                    if (row != null)
                    {
                        String vid = row.getString("vid");
                        String name = row.getString("name");
                        map_vertex_name.put(vid, name);
                    }
                    doneSignal.countDown();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    doneSignal.countDown();
                }
            },
                    ceName
            );

            ResultSetFuture resultSetFuture_color = session.executeAsync(statement_color.bind(s_i, timestamp));
            Futures.addCallback(resultSetFuture_color,
                    new FutureCallback<ResultSet>()
            {
                @Override
                public void onSuccess(ResultSet result)
                {
                    Row row = result.one();
                    if (row != null)
                    {
                        String vid = row.getString("vid");
                        String color = row.getString("color");
                        map_vertex_color.put(vid, color);
                    }
                    doneSignal.countDown();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    doneSignal.countDown();
                }
            },
                    ceColor
            );

            ResultSetFuture resultSetFuture_out_label = session.executeAsync(statement_out_label.bind(s_i, timestamp));
            Futures.addCallback(resultSetFuture_out_label,
                    new FutureCallback<ResultSet>()
            {
                @Override
                public void onSuccess(ResultSet result)
                {
                    Map<String, TreeMap<String, String>> all_attribute = new HashMap<String, TreeMap<String, String>>(); // [targetID, [timestamp,label] ]

                    List<Row> rows = result.all();

                    String sourceID = s_i;
                    for (Row row : rows)
                    {
                        sourceID = row.getString("sourceID");
                        String label = row.getString("label");
                        String rowtimestamp = row.getString("timestamp");
                        String ID = row.getString("targetID");

                        if (!all_attribute.containsKey(ID))
                        {
                            TreeMap<String, String> temp = new TreeMap<String, String>();
                            all_attribute.put(ID, temp);
                        }
                        TreeMap<String, String> changes = all_attribute.get(ID);
                        changes.put(rowtimestamp, label);
                        all_attribute.put(ID, changes);
                    }

                    map_edge_out_label.put(sourceID, all_attribute);
                    doneSignal.countDown();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    doneSignal.countDown();
                }
            },
                    ceOutLabel
            );

            ResultSetFuture resultSetFuture_out_weight = session.executeAsync(statement_out_weight.bind(s_i, timestamp));
            Futures.addCallback(resultSetFuture_out_weight,
                    new FutureCallback<ResultSet>()
            {
                @Override
                public void onSuccess(ResultSet result)
                {
                    Map<String, TreeMap<String, String>> all_attribute = new HashMap<String, TreeMap<String, String>>();

                    List<Row> rows = result.all();

                    String sourceID = s_i;
                    for (Row row : rows)
                    {
                        sourceID = row.getString("sourceID");
                        String label = row.getString("weight");
                        String rowtimestamp = row.getString("timestamp");
                        String ID = row.getString("targetID");

                        if (!all_attribute.containsKey(ID))
                        {
                            TreeMap<String, String> temp = new TreeMap<String, String>();
                            all_attribute.put(ID, temp);
                        }
                        TreeMap<String, String> changes = all_attribute.get(ID);
                        changes.put(rowtimestamp, label);
                        all_attribute.put(ID, changes);
                    }

                    map_edge_out_weight.put(sourceID, all_attribute);
                    doneSignal.countDown();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    doneSignal.countDown();
                }
            },
                    ceOutWeight
            );

            ResultSetFuture resultSetFuture_in_label = session.executeAsync(statement_in_label.bind(s_i, timestamp));
            Futures.addCallback(resultSetFuture_in_label,
                    new FutureCallback<ResultSet>()
            {
                @Override
                public void onSuccess(ResultSet result)
                {
                    Map<String, TreeMap<String, String>> all_attribute = new HashMap<String, TreeMap<String, String>>();

                    List<Row> rows = result.all();

                    String targetID = s_i;
                    for (Row row : rows)
                    {
                        targetID = row.getString("targetID");
                        String label = row.getString("label");
                        String rowtimestamp = row.getString("timestamp");
                        String ID = row.getString("sourceID");

                        if (!all_attribute.containsKey(ID))
                        {
                            TreeMap<String, String> temp = new TreeMap<String, String>();
                            all_attribute.put(ID, temp);
                        }
                        TreeMap<String, String> changes = all_attribute.get(ID);
                        changes.put(rowtimestamp, label);
                        all_attribute.put(ID, changes);
                    }

                    map_edge_in_label.put(targetID, all_attribute);
                    doneSignal.countDown();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    doneSignal.countDown();
                }
            },
                    ceInLabel
            );

            ResultSetFuture resultSetFuture_in_weight = session.executeAsync(statement_in_weight.bind(s_i, timestamp));
            Futures.addCallback(resultSetFuture_in_weight,
                    new FutureCallback<ResultSet>()
            {
                @Override
                public void onSuccess(ResultSet result)
                {
                    Map<String, TreeMap<String, String>> all_attribute = new HashMap<String, TreeMap<String, String>>();

                    List<Row> rows = result.all();

                    String targetID = s_i;
                    for (Row row : rows)
                    {
                        targetID = row.getString("targetID");
                        String label = row.getString("weight");
                        String rowtimestamp = row.getString("timestamp");
                        String ID = row.getString("sourceID");

                        if (!all_attribute.containsKey(ID))
                        {
                            TreeMap<String, String> temp = new TreeMap<String, String>();
                            all_attribute.put(ID, temp);
                        }
                        TreeMap<String, String> changes = all_attribute.get(ID);
                        changes.put(rowtimestamp, label);
                        all_attribute.put(ID, changes);
                    }

                    map_edge_in_weight.put(targetID, all_attribute);
                    doneSignal.countDown();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    doneSignal.countDown();
                }
            },
                    ceInWeight
            );

            ResultSetFuture resultSetFuture_out = session.executeAsync(statement_out.bind(s_i, timestamp));
            Futures.addCallback(resultSetFuture_out,
                    new FutureCallback<ResultSet>()
            {
                @Override
                public void onSuccess(ResultSet result)
                {
                    String sourceID = s_i;
                    map_edge_out.put(sourceID, result);
                    doneSignal.countDown();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    doneSignal.countDown();
                }
            },
                    ceOut
            );

            ResultSetFuture resultSetFuture_in = session.executeAsync(statement_in.bind(s_i, timestamp));
            Futures.addCallback(resultSetFuture_in,
                    new FutureCallback<ResultSet>()
            {
                @Override
                public void onSuccess(ResultSet result)
                {
                    String targetID = s_i;
                    map_edge_in.put(targetID, result);
                    doneSignal.countDown();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    doneSignal.countDown();
                }
            },
                    ceIn
            );
        }

        try
        {
            doneSignal.await(); // Wait until all async queries have finished
        } catch (InterruptedException ex)
        {
            Logger.getLogger(MultipleTablesModel.class.getName()).log(Level.SEVERE, null, ex);
        }

        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time elapsed for fetching data: " + elapsedSeconds + " seconds");

        tStart = System.currentTimeMillis();
        for (Integer i : vids)
        {
            String vid = Integer.toString(i);

            String vertex_name = map_vertex_name.get(vid);
            String vertex_color = map_vertex_color.get(vid);

            Map<String, TreeMap<String, String>> all_outgoing_labels = map_edge_out_label.get(vid);
            Map<String, TreeMap<String, String>> all_outgoing_weights = map_edge_out_weight.get(vid);
            Map<String, TreeMap<String, String>> all_incoming_labels = map_edge_in_label.get(vid);
            Map<String, TreeMap<String, String>> all_incoming_weights = map_edge_in_weight.get(vid);

            Map<String, Map<String, Edge>> allEdges = new HashMap<String, Map<String, Edge>>();

            // First retrieve the outgoing edges of the vertex
            ResultSet rs = map_edge_out.get(vid);

            List<Row> rows = rs.all();

            Map<String, Edge> outgoing_edges = new HashMap<String, Edge>();

            for (Row row : rows)
            {
                String end = row.getString("end");
                if (Integer.parseInt(timestamp) > Integer.parseInt(end))
                {
                    continue;
                }
                String start = row.getString("start");
                String targetID = row.getString("targetID");
                TreeMap<String, String> out_labels = all_outgoing_labels.get(targetID);
                String label = getLastValue(out_labels, timestamp);
                TreeMap<String, String> out_weights = all_outgoing_weights.get(targetID);
                String weight = getLastValue(out_weights, timestamp);

                Edge newedge = new Edge(label, weight, targetID, start, end);
                outgoing_edges.put(targetID, newedge);
            }

            // Then retrieve the incoming edges of the vertex
            rs = map_edge_in.get(vid);

            rows = rs.all();

            Map<String, Edge> incoming_edges = new HashMap<String, Edge>();

            for (Row row : rows)
            {
                String end = row.getString("end");
                if (Integer.parseInt(timestamp) > Integer.parseInt(end))
                {
                    continue;
                }
                String start = row.getString("start");
                String sourceID = row.getString("sourceID");
                TreeMap<String, String> in_labels = all_incoming_labels.get(sourceID);
                String label = getLastValue(in_labels, timestamp);
                TreeMap<String, String> in_weights = all_incoming_weights.get(sourceID);
                String weight = getLastValue(in_weights, timestamp);

                Edge newedge = new Edge(label, weight, sourceID, start, end);
                incoming_edges.put(sourceID, newedge);
            }

            allEdges.put("outgoing_edges", outgoing_edges);
            allEdges.put("incoming_edges", incoming_edges);

            Vertex v = new Vertex();
            v.setVid(vid);
            v.setTimestamp(timestamp);
            v.setOutgoing_edges(allEdges.get("outgoing_edges"));
            v.setIncoming_edges(allEdges.get("incoming_edges"));
            v.setValue("name", vertex_name);
            v.setValue("color", vertex_color);

            results.add(v);
        }
        ceName.shutdown();
        ceColor.shutdown();
        ceOutLabel.shutdown();
        ceOutWeight.shutdown();
        ceInLabel.shutdown();
        ceInWeight.shutdown();
        ceOut.shutdown();
        ceIn.shutdown();

        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time elapsed for transforming the data: " + elapsedSeconds + " seconds.");

        return results;
    }

    @Override
    public HashMap<String, HashMap<String, Integer>> getDegreeDistribution(String first, String last)
    {
        long tStart, tEnd, tDelta;

        HashMap<String, HashMap<Integer, Integer>> vertexDegreeInAllInstances = new HashMap<String, HashMap<Integer, Integer>>(); // Holds for each vertex a map containing (Instance,Vertex_count) pairs

        HashMap<String, ArrayList<String>> vertices = getAllAliveVertices(first, last);
        ArrayList<String> allVertices = vertices.get("allVertices");

        PreparedStatement statement = session.prepare("SELECT start, end, sourceID FROM " + keyspace + ".edge_outgoing WHERE sourceID = ?");

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
            List<Row> allrows = rs.all();
            rows.addAll(allrows);

        }
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for retrieving the edges from the relevant alive nodes: " + elapsedSeconds + " seconds.");

        tStart = System.currentTimeMillis();
        for (Row row : rows)
        {
            String rowend = row.getString("end");
            if (Integer.parseInt(rowend) < Integer.parseInt(first)) // That means that the diachronic node's "start" and "end" time instances were BOTH before our query instance
            {
                continue;
            }

            String rowstart = row.getString("start");
            String vid = row.getString("sourceID");

            int start = Math.max(Integer.parseInt(first), Integer.parseInt(rowstart)); // Only report values that are after both "first" and the diachronic node's "rowstart"
            int end = Math.min(Integer.parseInt(last), Integer.parseInt(rowend)); // Only report values that are before both "last" and the diachronic node's "rowend"

            if (!vertexDegreeInAllInstances.containsKey(vid))
            {
                vertexDegreeInAllInstances.put(vid, new HashMap<Integer, Integer>());
            }
            HashMap<Integer, Integer> vertexDegrees = vertexDegreeInAllInstances.get(vid);
            for (int i = start; i <= end; i++)
            {
                if (!vertexDegrees.containsKey(i))
                {
                    vertexDegrees.put(i, 0);
                }
                vertexDegrees.put(i, vertexDegrees.get(i) + 1);
            }
        }
        System.out.println("First map finished.");

        HashMap<Integer, List<Integer>> allDegreesPerTimeInstance = new HashMap<Integer, List<Integer>>(); // (Instance, <Degree1, Degree2, ...>)
        for (String s_vid : vertexDegreeInAllInstances.keySet())
        {
            HashMap<Integer, Integer> s_vertexDegrees = vertexDegreeInAllInstances.get(s_vid);

            for (Integer instance : s_vertexDegrees.keySet())
            {
                Integer degree = s_vertexDegrees.get(instance);
                if (!allDegreesPerTimeInstance.containsKey(instance))
                {
                    allDegreesPerTimeInstance.put(instance, new ArrayList<Integer>());
                }
                allDegreesPerTimeInstance.get(instance).add(degree);
            }
        }
        System.out.println("Second map finished.");

        HashMap<String, HashMap<String, Integer>> results = new HashMap<String, HashMap<String, Integer>>();
        for (Integer instance : allDegreesPerTimeInstance.keySet())
        {
            List<Integer> degrees = allDegreesPerTimeInstance.get(instance);

            HashMap<String, Integer> degreeDistr = new HashMap<String, Integer>();
            for (Integer degree : degrees)
            {
                Integer count = degreeDistr.get(degree.toString());
                degreeDistr.put(degree.toString(), (count == null) ? 1 : count + 1);
            }
            results.put("" + instance, degreeDistr);
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

        HashMap<String, HashMap<String, Double>> vertexDegreeInAllInstances = new HashMap<String, HashMap<String, Double>>(); // Holds for each vertex a map containing (Instance,Vertex_count) pairs

        ConcurrentLinkedQueue<Row> rows = getAllEdgesAndFilterAlive(first, last);

        tStart = System.currentTimeMillis();
        for (Row row : rows)
        {
            String rowend = row.getString("end");
            if (Integer.parseInt(rowend) < Integer.parseInt(first)) // That means that the diachronic node's "start" and "end" time instances were BOTH before our query instance
            {
                continue;
            }

            String rowstart = row.getString("start");
            String vid = row.getString("sourceID");

            int start = Math.max(Integer.parseInt(first), Integer.parseInt(rowstart)); // Only report values that are after both "first" and the diachronic node's "rowstart"
            int end = Math.min(Integer.parseInt(last), Integer.parseInt(rowend)); // Only report values that are before both "last" and the diachronic node's "rowend"

            if (!vertexDegreeInAllInstances.containsKey(vid))
            {
                vertexDegreeInAllInstances.put(vid, new HashMap<String, Double>());
            }
            HashMap<String, Double> vertexDegrees = vertexDegreeInAllInstances.get(vid);
            for (int i = start; i <= end; i++)
            {
                if (!vertexDegrees.containsKey("" + i))
                {
                    vertexDegrees.put("" + i, 0.0);
                }
                vertexDegrees.put("" + i, vertexDegrees.get("" + i) + 1.0);
            }
        }

        HashMap<String, List<Double>> allDegreesPerTimeInstance = new HashMap<String, List<Double>>(); // (Instance, <Degree1, Degree2, ...>)
        for (String s_vid : vertexDegreeInAllInstances.keySet())
        {
            HashMap<String, Double> s_vertexDegrees = vertexDegreeInAllInstances.get(s_vid);

            for (String instance : s_vertexDegrees.keySet())
            {
                Double degree = s_vertexDegrees.get(instance);
                if (!allDegreesPerTimeInstance.containsKey(instance))
                {
                    allDegreesPerTimeInstance.put(instance, new ArrayList<Double>());
                }
                allDegreesPerTimeInstance.get(instance).add(degree);
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
        System.out.println("Time required for processing and evaluating the DegDistr query: " + elapsedSeconds + " seconds.");
        return results;
    }

    private List<Interval> getIntervalsForAttribute(String attr, String vid, String first, String last)
    {
        ArrayList<Integer> instances = new ArrayList<Integer>();

        ResultSet result = session.execute("SELECT * FROM " + keyspace + ".vertex_" + attr + " WHERE vid = '" + vid + "' AND timestamp <= '" + last + "';");
        TreeMap<String, String> attrPerInstance = new TreeMap<String, String>();
        while (!result.isExhausted())
        {
            Row row = result.one();
            String timestamp = row.getString("timestamp");

            attrPerInstance.put(timestamp, row.getString(attr));
            instances.add(Integer.valueOf(timestamp));
        }
        Collections.sort(instances);
        List<Interval> attrsList = new ArrayList<Interval>();
        String left, right = null, value;
        for (int i = 0; i < instances.size() - 1; i++)
        {
            left = "" + instances.get(i);
            right = "" + instances.get(i + 1);
            value = attrPerInstance.get(left);
            Interval ival = new Interval(value, left, right);
            if (ival.stab(first) || Integer.valueOf(left) > Integer.valueOf(first))
            {
                attrsList.add(ival);
            }
        }
        if (instances.size() == 1)
        {
            value = attrPerInstance.get("" + instances.get(0));
            left = "" + instances.get(0);
            right = last;
            attrsList.add(new Interval(value, left, right));
        } else
        {
            attrsList.add(new Interval(attrPerInstance.get(right), right, last));
        }
        return attrsList;
    }

    private String getLastValue(TreeMap<String, String> attributes, String timestamp)
    {
        String last = "-2";
        for (String ts : attributes.keySet())
        {
            if (Integer.parseInt(ts) <= Integer.parseInt(timestamp))
            {
                last = attributes.get(ts);
            } else
            {
                break;
            }
        }
        return last;
    }

    @Override
    public List<String> getOneHopNeighborhood(String vid, String first, String last)
    {
        long tStart, tEnd, tDelta;
        List<String> results = new ArrayList<String>();

        tStart = System.currentTimeMillis();
        ResultSet rs = session.execute("SELECT end, targetID FROM " + keyspace + ".edge_outgoing "
                + "WHERE sourceID = '" + vid + "' "
                + "AND start <= '" + last + "'");

        List<Row> rows = rs.all();
        tEnd = System.currentTimeMillis();
        tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("Time required for retrieving all the relevant outgoing edges: " + elapsedSeconds + " seconds, (OneHop on VID, Timestamps: [" + vid + ", " + first + " to " + last + "])");

        tStart = System.currentTimeMillis();
        for (Row row : rows)
        {
            String end = row.getString("end");
            if (Integer.parseInt(first) > Integer.parseInt(end))
            {
                continue;
            }
            String targetID = row.getString("targetID");

            results.add(targetID);
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
        DiaNode dn = new DiaNode(vid);

        ResultSet result = session.execute("SELECT start, end FROM " + keyspace + ".vertex WHERE vid = '" + vid + "';");
        Row row = result.one();
        String rowstart = row.getString("start");
        String rowend = row.getString("end");
        dn.setStart(rowstart);
        dn.setEnd(rowend);

        // First retrieve the attributes of the vertex for [first, last)
        List<Interval> namesList = getIntervalsForAttribute("name", vid, first, last);
        List<Interval> colorsList = getIntervalsForAttribute("color", vid, first, last);

        // Then retrieve the outgoing edges of the vertex for [first, last) along with their attributes
        Map<String, List<Edge>> outgoing_edges = new HashMap<String, List<Edge>>();
        Map<String, TreeMap<String, String>> all_outgoing_labels = getAttributeOfDirectedEdge(vid, last, "label", "outgoing"); // [target, [timestamp, label]]
        Map<String, TreeMap<String, String>> all_outgoing_weights = getAttributeOfDirectedEdge(vid, last, "weight", "outgoing"); // [target, [timestamp, weight]]
        ResultSet rs = session.execute("SELECT * FROM " + keyspace + ".edge_outgoing " + "WHERE sourceID = '" + vid + "' " + "AND start <= '" + last + "'");
        List<Row> rows = rs.all();
        for (Row edge : rows)
        {
            String end = edge.getString("end");
            if (Integer.parseInt(end) < Integer.parseInt(first))
            {
                continue;
            }
            String start = edge.getString("start");
            String targetID = edge.getString("targetID");
            TreeMap<String, String> out_labels = all_outgoing_labels.get(targetID);
            if (out_labels == null) // Hack. There shouldn't be an edge without the corresponding attributes.
            {
                out_labels = new TreeMap<String, String>();
                out_labels.put("labelNotFound", "valueNotFound");
            }
            TreeMap<String, String> out_weights = all_outgoing_weights.get(targetID);
            if (out_weights == null) // Hack. There shouldn't be an edge without the corresponding attributes.
            {
                out_weights = new TreeMap<String, String>();
                out_weights.put("weightNotFound", "valueNotFound");
            }
            List<Edge> edgesList = transformToEdges(targetID, start, end, out_labels, out_weights);
            outgoing_edges.put(targetID, edgesList);
        }

        // Then retrieve the incoming edges of the vertex for [first, last) along with their attributes
        Map<String, List<Edge>> incoming_edges = new HashMap<String, List<Edge>>();
        rs = session.execute("SELECT * FROM " + keyspace + ".edge_incoming " + "WHERE targetID = '" + vid + "' " + "AND start <= '" + last + "'");
        Map<String, TreeMap<String, String>> all_incoming_labels = getAttributeOfDirectedEdge(vid, last, "label", "incoming"); // [source, [timestamp, label]]
        Map<String, TreeMap<String, String>> all_incoming_weights = getAttributeOfDirectedEdge(vid, last, "weight", "incoming"); // [source, [timestamp, weight]]        
        rows = rs.all();
        for (Row edge : rows)
        {
            String end = edge.getString("end");
            if (Integer.parseInt(end) < Integer.parseInt(first))
            {
                continue;
            }
            String start = edge.getString("start");
            String sourceID = edge.getString("sourceID");
            TreeMap<String, String> in_labels = all_incoming_labels.get(sourceID);
            if (in_labels == null) // Hack. There shouldn't be an edge without the corresponding attributes.
            {
                in_labels = new TreeMap<String, String>();
                in_labels.put("labelNotFound", "valueNotFound");
            }
            TreeMap<String, String> in_weights = all_incoming_weights.get(sourceID);
            if (in_weights == null) // Hack. There shouldn't be an edge without the corresponding attributes.
            {
                in_weights = new TreeMap<String, String>();
                in_weights.put("weightNotFound", "valueNotFound");
            }
            List<Edge> edgesList = transformToEdges(sourceID, start, end, in_labels, in_weights);
            incoming_edges.put(sourceID, edgesList);
        }

        // Finally, add all details in the diachronic node and return it
        dn.insertAttribute("name", namesList);
        dn.insertAttribute("color", colorsList);
        dn.setOutgoing_edges(outgoing_edges);
        dn.setIncoming_edges(incoming_edges);
        dn.keepValuesInInterval(first, last);

        return dn;
    }

    public Vertex getVertexInstance(String vid, String timestamp)
    {
        Map<String, String> vertexNameMap = read("vertex_name", "vid", vid, timestamp);
        Map<String, String> vertexColorMap = read("vertex_color", "vid", vid, timestamp);
        Map<String, Map<String, Edge>> allEdges = getAllEdgesOfVertex(vid, timestamp);

        Vertex v = new Vertex();
        v.setVid(vid);
        v.setTimestamp(timestamp);
        v.setOutgoing_edges(allEdges.get("outgoing_edges"));
        v.setIncoming_edges(allEdges.get("incoming_edges"));
        v.setValue("name", vertexNameMap.get("name"));
        v.setValue("color", vertexColorMap.get("color"));

        return v;
    }

    public void insert(String table, HashMap<String, String> values)
    {
        try
        {
            Insert insertStmt = QueryBuilder.insertInto(table);

            // Add fields
            for (Map.Entry<String, String> entry : values.entrySet())
            {
                String value = entry.getValue();
                insertStmt.value(entry.getKey(), value);
            }

            insertStmt.setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).enableTracing();

            ResultSet rs = session.execute(insertStmt);

        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void insertEdge(String sourceID, String targetID, String start, String end, String label, String weight)
    {
        HashMap<String, String> values = new HashMap<String, String>();
        values.put("start", start);
        values.put("end", end);
        values.put("sourceID", sourceID);
        values.put("targetID", targetID);
        insert("edge_outgoing", values);
        values.clear();

        values.put("label", label);
        values.put("timestamp", start);
        values.put("sourceID", sourceID);
        values.put("targetID", targetID);
        insert("edge_label_outgoing", values);
        values.clear();

        values.put("weight", weight);
        values.put("timestamp", start);
        values.put("sourceID", sourceID);
        values.put("targetID", targetID);
        insert("edge_weight_outgoing", values);
        values.clear();

        values.put("start", start);
        values.put("end", end);
        values.put("sourceID", sourceID);
        values.put("targetID", targetID);
        insert("edge_incoming", values);
        values.clear();

        values.put("label", label);
        values.put("timestamp", start);
        values.put("sourceID", sourceID);
        values.put("targetID", targetID);
        insert("edge_label_incoming", values);
        values.clear();

        values.put("weight", weight);
        values.put("timestamp", start);
        values.put("sourceID", sourceID);
        values.put("targetID", targetID);
        insert("edge_weight_incoming", values);
        values.clear();
    }

    @Override
    public void insertVertex(String vid, String name, String start, String end, String color)
    {
        HashMap<String, String> values = new HashMap<String, String>();

        values.put("vid", vid);
        values.put("start", start);
        values.put("end", end);
        insert("vertex", values);
        values.clear();

        values.put("vid", vid);
        values.put("name", name);
        values.put("timestamp", start);
        insert("vertex_name", values);
        values.clear();

        values.put("vid", vid);
        values.put("color", color);
        values.put("timestamp", start);
        insert("vertex_color", values);
        values.clear();
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
                {
                    continue;
                }

                if (line.equals("graph 1 0") || line.equals("graph 1")) // As soon as we've reached the second snapshot, stop
                {
                    break;
                }

                if (line.equals("use " + keyspace + ""))
                {
                    session.execute("USE " + keyspace + ";");
                } else if (line.startsWith("vertex"))
                {
                    tokens = line.split(" ");
                    String verID = tokens[1];
                    String name, color;
                    if (tokens.length >= 3)
                    {
                        name = tokens[2].split("=")[1].replaceAll("\"", "");
                    } else
                    {
                        name = getRandomString(4);
                    }
                    if (tokens.length == 4)
                    {
                        color = tokens[3].split("=")[1].replaceAll("\"", "");
                    } else
                    {
                        color = getRandomString(4);
                    }
                    Vertex ver = new Vertex();
                    ver.setVid(verID);
                    ver.setTimestamp("00000000");
                    HashMap<String, String> attributes = new HashMap<String, String>();
                    attributes.put("name", name);
                    attributes.put("color", color);
                    ver.setAttributes(attributes);
                    vertices.put(verID, ver);
                } else if (line.startsWith("edge"))
                {
                    tokens = line.split(" ");
                    String sourceID = tokens[1];
                    String targetID = tokens[2];
                    String weight;
                    if (tokens.length == 4)
                    {
                        weight = tokens[3].split("=")[1];
                    } else
                    {
                        weight = "" + Math.random();
                    }

                    Vertex sVer = vertices.get(sourceID);
                    sVer.addOutgoingEdge(targetID, new Edge("testlabel", weight, targetID, "00000000", DataModel.padWithZeros("" + snap_count)));
                    Vertex tVer = vertices.get(targetID);
                    tVer.addIncomingEdge(sourceID, new Edge("testlabel", weight, sourceID, "00000000", DataModel.padWithZeros("" + snap_count)));
                    vertices.put(sourceID, sVer);
                    vertices.put(targetID, tVer);
                }
            }
            file.close();

            for (String vid : vertices.keySet())
            {
                session.executeAsync("INSERT INTO " + keyspace + ".vertex");
            }

            for (String vid : vertices.keySet())
            {
                Vertex ver = vertices.get(vid);
                HashMap<String, String> attrs = ver.getAttributes();

                String start = ver.getTimestamp();
                String end = DataModel.padWithZeros("" + snap_count);
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
                {
                    allIncEdgesstr = allIncEdgesstr.substring(0, allIncEdgesstr.length() - 2);
                }

                String allOutEdgesstr = "";
                for (String target : allOutEdges.keySet())
                {
                    Edge edge = allOutEdges.get(target);
                    allOutEdgesstr = allOutEdgesstr.concat("'" + edge.otherEnd + "': [");
                    allOutEdgesstr = allOutEdgesstr.concat(edge.toString()).concat("], ");
                }
                if (!allOutEdgesstr.equals(""))
                {
                    allOutEdgesstr = allOutEdgesstr.substring(0, allOutEdgesstr.length() - 2);
                }

                session.execute("INSERT INTO " + keyspace + ".dianode (vid, start, end, name, color, incoming_edges, outgoing_edges)" //Assume only one edge per vertex pair in graph 0
                        + "VALUES ('" + ver.getVid() + "', '" + start + "', '" + end + "', "
                        + "[{value: '" + name + "', start: '00000000', end: '" + DataModel.padWithZeros("" + snap_count) + "'}], "
                        + "[{value: '" + color + "', start: '00000000', end: '" + DataModel.padWithZeros("" + snap_count) + "'}], "
                        + "{" + allIncEdgesstr + "}, "
                        + "{" + allOutEdgesstr + "}"
                        + ");");
            }
        } catch (FileNotFoundException ex)
        {
            Logger.getLogger(SingleTableModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex)
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

            BufferedReader file = new BufferedReader(new FileReader(input));
            String line, curVersion = "0";
            String tokens[];
            int verKcounter = 0;
            int edgeKcounter = 0;

            while ((line = file.readLine()) != null)
            {
                if (line.startsWith("mkdir") || line.startsWith("cd") || line.startsWith("time") || line.startsWith("string") || line.startsWith("double") || line.startsWith("shutdown"))
                {
                    continue;
                }

                if (line.startsWith("graph"))
                {
                    System.out.println(line);
                    tokens = line.split(" ");
                    if (tokens.length == 2) // "graph X" statement
                    {
                        curVersion = tokens[1];
                    } else if (tokens.length == 3) // "graph X Y" statement
                    {
                        curVersion = tokens[1];
                    }
                } else if (line.startsWith("vertex"))
                {
                    tokens = line.split(" ");
                    String verID = tokens[1];
                    String name, color;
                    if (tokens.length >= 3)
                    {
                        name = tokens[2].split("=")[1].replaceAll("\"", "");
                    } else
                    {
                        name = getRandomString(4);
                    }
                    if (tokens.length == 4)
                    {
                        color = tokens[3].split("=")[1].replaceAll("\"", "");
                    } else
                    {
                        color = getRandomString(4);
                    }
                    insertVertex(verID, name, DataModel.padWithZeros(curVersion), DataModel.padWithZeros("" + snap_count), color);
                    verKcounter++;
                    if (verKcounter % 1000 == 0)
                    {
                        System.out.println("Vertices processed: " + verKcounter);
                    }
                } else if (line.startsWith("edge"))
                {
                    tokens = line.split(" ");
                    String sourceID = tokens[1];
                    String targetID = tokens[2];
                    String weight;
                    if (tokens.length == 4)
                    {
                        weight = tokens[3].split("=")[1];
                    } else
                    {
                        weight = "" + Math.random();
                    }
                    insertEdge(sourceID, targetID, DataModel.padWithZeros(curVersion), DataModel.padWithZeros("" + snap_count), getRandomString(3), weight);
                    edgeKcounter++;
                    if (edgeKcounter % 1000 == 0)
                    {
                        System.out.println("Edges processed: " + edgeKcounter);
                    }
                } else if (line.startsWith("update vertex"))
                {
                    tokens = line.split(" ");
                    String verID = tokens[2];
                    String attrName = tokens[3].split("=")[0];
                    String value = tokens[3].split("=")[1].replaceAll("\"", "");
                    updateVertexAttribute(verID, attrName, value, curVersion);
                }
            }
        } catch (FileNotFoundException ex)
        {
            Logger.getLogger(MultipleTablesModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex)
        {
            Logger.getLogger(MultipleTablesModel.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public Map<String, String> read(String table, String keyName, String keyValue)
    {
        try
        {
            Statement stmt;
            Select.Builder selectBuilder;

            selectBuilder = QueryBuilder.select().all();

            stmt = selectBuilder.from(table).where(QueryBuilder.eq(keyName, keyValue)).limit(1);
            stmt.setConsistencyLevel(READ_CONSISTENCY_LEVEL);

            ResultSet rs = session.execute(stmt);

            HashMap<String, String> resultRow = new HashMap<String, String>();

            for (Row row : rs.all())
            {
                ColumnDefinitions cd = row.getColumnDefinitions();

                for (ColumnDefinitions.Definition def : cd)
                {
                    ByteBuffer val = row.getBytesUnsafe(def.getName());
                    if (val != null)
                    {
                        resultRow.put(def.getName(), bb_to_str(val));
                    } else
                    {
                        resultRow.put(def.getName(), null);
                    }
                }
            }
            return resultRow;
        } catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("Error reading key: " + keyName);
        }
        return Collections.EMPTY_MAP;
    }

    public Map<String, String> read(String table, String keyName, String keyValue, String timestamp)
    {
        try
        {
            Statement stmt;
            Select.Builder selectBuilder;

            selectBuilder = QueryBuilder.select().all();

            stmt = selectBuilder.from(table).where(QueryBuilder.eq(keyName, keyValue)).and(QueryBuilder.lte("timestamp", timestamp)).limit(1);
            stmt.setConsistencyLevel(READ_CONSISTENCY_LEVEL);

            ResultSet rs = session.execute(stmt);

            HashMap<String, String> resultRow = new HashMap<String, String>();
            // Should be only 1 row
            for (Row row : rs.all())
            {
                ColumnDefinitions cd = row.getColumnDefinitions();

                for (ColumnDefinitions.Definition def : cd)
                {
                    ByteBuffer val = row.getBytesUnsafe(def.getName());
                    if (val != null)
                    {
                        resultRow.put(def.getName(), bb_to_str(val));
                    } else
                    {
                        resultRow.put(def.getName(), null);
                    }
                }
            }
            return resultRow;
        } catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("Error reading key: " + keyName);
        }
        return Collections.EMPTY_MAP;
    }

    public List<Map<String, String>> read(String table, String keyName, String keyValue, String start, String end)
    {
        try
        {
            Statement stmt;
            Select.Builder selectBuilder;

            selectBuilder = QueryBuilder.select().all();

            stmt = selectBuilder.from(table).where(QueryBuilder.eq(keyName, keyValue)).and(QueryBuilder.gte("timestamp", start)).and(QueryBuilder.lte("timestamp", end));
            stmt.setConsistencyLevel(READ_CONSISTENCY_LEVEL);

            ResultSet rs = session.execute(stmt);

            List<Map<String, String>> result = new ArrayList<Map<String, String>>();
            HashMap<String, String> resultRow;

            for (Row row : rs.all())
            {
                resultRow = new HashMap<String, String>();
                ColumnDefinitions cd = row.getColumnDefinitions();

                for (ColumnDefinitions.Definition def : cd)
                {
                    ByteBuffer val = row.getBytesUnsafe(def.getName());
                    if (val != null)
                    {
                        resultRow.put(def.getName(), bb_to_str(val));
                    } else
                    {
                        resultRow.put(def.getName(), null);
                    }
                }
                result.add(resultRow);
            }
            return result;
        } catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("Error reading key: " + keyName);
        }
        return Collections.EMPTY_LIST;
    }

    private List<Edge> transformToEdges(String otherEnd, String start, String end, TreeMap<String, String> labels, TreeMap<String, String> weights)
    {
        List<Edge> allEdges = new ArrayList<Edge>();

        // Get all the instances where a change occurs (in either out_labels or out_weights)
        TreeSet<String> allInstances = new TreeSet<String>();
        allInstances.addAll(labels.keySet());
        allInstances.addAll(weights.keySet());

        Edge edge = new Edge(otherEnd);
        edge.start = start;
        edge.label = labels.get(allInstances.first());
        edge.weight = weights.get(allInstances.first());
        String prevLabel = edge.label;
        String prevWeight = edge.weight;

        for (String instance : allInstances)
        {
            String label, weight;
            if (labels.containsKey(instance))
            {
                label = labels.get(instance);
            } else
            {
                label = prevLabel;
            }
            if (weights.containsKey(instance))
            {
                weight = weights.get(instance);
            } else
            {
                weight = prevWeight;
            }

            if (!label.equals(prevLabel) && !weight.equals(prevWeight))
            {
                edge.end = instance;
                allEdges.add(edge);
                edge = new Edge(otherEnd);
                edge.start = instance;
                edge.label = label;
                edge.weight = weight;
                prevLabel = label;
                prevWeight = weight;
            } else if (!label.equals(prevLabel))
            {
                edge.end = instance;
                allEdges.add(edge);
                edge = new Edge(otherEnd);
                edge.start = instance;
                edge.label = label;
                edge.weight = prevWeight;
                prevLabel = label;
            } else if (!weight.equals(prevWeight))
            {
                edge.end = instance;
                allEdges.add(edge);
                edge = new Edge(otherEnd);
                edge.start = instance;
                edge.label = prevLabel;
                edge.weight = weight;
                prevWeight = weight;
            }
        }
        edge.end = end;
        allEdges.add(edge);

        return allEdges;
    }

    @Override
    public void updateVertexAttribute(String vid, String attrName, String attrValue, String timestamp)
    {
        HashMap<String, String> values = new HashMap<String, String>();
        values.put("vid", vid);
        values.put(attrName, attrValue);
        values.put("timestamp", timestamp);
        insert("vertex_" + attrName, values);
        values.clear();
    }

    @Override
    public void useKeyspace()
    {
        session.execute("USE " + keyspace + ";");
    }

}

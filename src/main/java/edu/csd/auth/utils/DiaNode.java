package edu.csd.auth.utils;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.google.common.reflect.TypeToken;
import edu.csd.auth.models.SingleTableModel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DiaNode
{
    private String vid;
    private String start;
    private String end;
    private Map<String, List<Interval>> attributes;
    private Map<String, List<Edge>> outgoing_edges;

    private Map<String, List<Edge>> incoming_edges;
    
    public DiaNode(String vid)
    {
        this.vid = vid;
        this.start = "0";
        this.end = String.valueOf(Integer.MAX_VALUE);
        this.attributes = new HashMap<String, List<Interval>>();
        this.outgoing_edges = new HashMap<String, List<Edge>>();
        this.incoming_edges = new HashMap<String, List<Edge>>();
    }

    public DiaNode(String vid, String start, String end, HashMap<String, List<Interval>> attributes, HashMap<String, List<Edge>> outgoing_edges, HashMap<String, List<Edge>> incoming_edges)
    {
        this.vid = vid;
        this.start = start;
        this.end = end;
        this.attributes = attributes;
        this.outgoing_edges = outgoing_edges;
        this.incoming_edges = incoming_edges;
    }
    
    public DiaNode(String vid, String start, String end)
    {
        this.vid = vid;
        this.start = start;
        this.end = end;
        this.attributes = new HashMap<String, List<Interval>>();
        this.outgoing_edges = new HashMap<String, List<Edge>>();
        this.incoming_edges = new HashMap<String, List<Edge>>();
    }
    
    public DiaNode(Row row)
    {
        this.vid = row.getString("vid");
        this.start = row.getString("start");
        this.end = row.getString("end");
        this.attributes = new HashMap<String, List<Interval>>();
        
        ColumnDefinitions cd = row.getColumnDefinitions();

        for (ColumnDefinitions.Definition def : cd)
        {
            if (def.getName().equals("vid") || def.getName().equals("start") || def.getName().equals("end") || def.getName().equals("incoming_edges") || def.getName().equals("outgoing_edges")) // We assume that every column apart from vid, start, end and the edges hold intervals
                continue;

            List<UDTValue> attrUDTList = row.getList(def.getName(), UDTValue.class);
            List<Interval> attrList = SingleTableModel.convertToIntervals(attrUDTList);
            
            attributes.put(def.getName(), attrList);
        }
        
        TypeToken<List<UDTValue>> listOfEdges = new TypeToken<List<UDTValue>>() {};
        Map<String, List<UDTValue>> edgesUDTList = row.getMap("outgoing_edges", TypeToken.of(String.class), listOfEdges);
        this.outgoing_edges = SingleTableModel.convertToEdgeList(edgesUDTList);
        
        listOfEdges = new TypeToken<List<UDTValue>>() {};
        edgesUDTList = row.getMap("incoming_edges", TypeToken.of(String.class), listOfEdges);
        this.incoming_edges = SingleTableModel.convertToEdgeList(edgesUDTList);
    }
    
    public Vertex convertToVertex(String timestamp)
    {
        Vertex ver = new Vertex();
        ver.setVid(this.vid);
        ver.setTimestamp(timestamp);
        for (String attrName : attributes.keySet())
        {
            String value = attributes.get(attrName).get(search(attrName, timestamp)).value;
            ver.setValue(attrName, value);
        }
        
        for (String targetID : outgoing_edges.keySet())
        {
            List<Edge> edges = outgoing_edges.get(targetID);
            for (Edge edge : edges)
                if (Integer.parseInt(timestamp) >= Integer.parseInt(edge.start) && Integer.parseInt(timestamp) < Integer.parseInt(edge.end))
                {
                    ver.addOutgoingEdge(targetID, edge);
                    break;
                }
        }
        for (String sourceID : incoming_edges.keySet())
        {
            List<Edge> edges = incoming_edges.get(sourceID);
            for (Edge edge : edges)
                if (Integer.parseInt(timestamp) >= Integer.parseInt(edge.start) && Integer.parseInt(timestamp) < Integer.parseInt(edge.end))
                {
                    ver.addIncomingEdge(sourceID, edge);
                    break;
                }
        }
        return ver;
    }

    public String getVid()
    {
        return vid;
    }

    public void setVid(String vid)
    {
        this.vid = vid;
    }

    public String getStart()
    {
        return start;
    }

    public void setStart(String start)
    {
        this.start = start;
    }

    public String getEnd()
    {
        return end;
    }

    public void setEnd(String end)
    {
        this.end = end;
    }

    public Map<String, List<Interval>> getAttributes()
    {
        return attributes;
    }

    public void setAttributes(HashMap<String, List<Interval>> attributes)
    {
        this.attributes = attributes;
    }
    
    public Map<String, List<Edge>> getOutgoing_edges() 
    {
        return outgoing_edges;
    }

    public void setOutgoing_edges(Map<String, List<Edge>> outgoing_edges) 
    {
        this.outgoing_edges = outgoing_edges;
    }
    
    public void setIncoming_edges(Map<String, List<Edge>> incoming_edges) 
    {
        this.incoming_edges = incoming_edges;
    }
    
    public void insertAttribute(String attrName, List<Interval> newAttributeValues)
    {
        if (attributes.containsKey(attrName))
            attributes.get(attrName).addAll(newAttributeValues);
        else
            attributes.put(attrName, newAttributeValues);
    }
    
    private int search(String attrName, String timestamp)
    {
        List<Interval> attrList = attributes.get(attrName);
        
        int index = Collections.binarySearch(attrList, new Interval(null, timestamp, String.valueOf(Integer.MAX_VALUE)));
        
        if (attrList.get(index).stab(timestamp))
            return index;
        if (index - 1 >= 0 && attrList.get(index-1).stab(timestamp))
            return index - 1;
        if (index + 1 <= attrList.size()-1 && attrList.get(index+1).stab(timestamp))
            return index + 1;
        
        return -1;
    }

    @Override
    public String toString()
    {
        return "DiaNode{" + "vid=" + vid + ", start=" + start + ", end=" + end + ", attributes=" + attributes + ", outgoing_edges=" + outgoing_edges + ", incoming_edges=" + incoming_edges + '}';
    }

    public void keepValuesInInterval(String first, String last)
    {
        start = (Integer.parseInt(start) < Integer.parseInt(first) ? first : start);
        end = (Integer.parseInt(end) > Integer.parseInt(last) ? last : end);
        for (String attrName : attributes.keySet())
        {
            List<Interval> allValues = attributes.get(attrName);
            Iterator<Interval> it = allValues.iterator();
            while (it.hasNext())
            {
                Interval ival = it.next();
                if (!(ival.stab(first) || ival.stab(last)))
                {
                    it.remove();
                    continue;
                }
                ival.start = (Integer.parseInt(ival.start) < Integer.parseInt(first) ? first : ival.start);
                ival.end = (Integer.parseInt(ival.end) > Integer.parseInt(last) ? last : ival.end);
            }
            attributes.put(attrName, allValues);
        }
        
        for (String targetID : outgoing_edges.keySet())
        {
            List<Edge> edges = outgoing_edges.get(targetID);
            Iterator<Edge> edgeIt = edges.iterator();
            while (edgeIt.hasNext())
            {
                Edge edge = edgeIt.next();
                if (Integer.parseInt(edge.end) < Integer.parseInt(first) || Integer.parseInt(last) < Integer.parseInt(edge.start))
                {
                    edgeIt.remove();
                    continue;
                }
                int lowerBound = (Integer.parseInt(edge.start) < Integer.parseInt(first) ? Integer.parseInt(first) : Integer.parseInt(edge.start));
                int upperBound = (Integer.parseInt(last) < Integer.parseInt(edge.end) ? Integer.parseInt(last) : Integer.parseInt(edge.end));
                edge.start = Integer.toString(lowerBound);
                edge.end = Integer.toString(upperBound);
            }
        }
        for (String sourceID : incoming_edges.keySet())
        {
            List<Edge> edges = incoming_edges.get(sourceID);
            Iterator<Edge> edgeIt = edges.iterator();
            while (edgeIt.hasNext()) 
            {
                Edge edge = edgeIt.next();
                if (Integer.parseInt(edge.end) < Integer.parseInt(first) || Integer.parseInt(last) < Integer.parseInt(edge.start)) 
                {
                    edgeIt.remove();
                    continue;
                }
                int lowerBound = (Integer.parseInt(edge.start) < Integer.parseInt(first) ? Integer.parseInt(first) : Integer.parseInt(edge.start));
                int upperBound = (Integer.parseInt(last) < Integer.parseInt(edge.end) ? Integer.parseInt(last) : Integer.parseInt(edge.end));
                edge.start = Integer.toString(lowerBound);
                edge.end = Integer.toString(upperBound);
            }
        }
    }
    
    public void merge(DiaNode dn)
    {
        this.end = dn.end;
        this.attributes.putAll(dn.attributes);
        this.incoming_edges.putAll(dn.incoming_edges);
        this.outgoing_edges.putAll(dn.outgoing_edges);
    }
    
}

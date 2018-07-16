package edu.csd.auth.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Vertex
{
    private String vid;
    private String timestamp;
    HashMap<String, String> attributes;
    HashMap<String, Edge> outgoing_edges;
    HashMap<String, Edge> incoming_edges;
    
    public Vertex()
    {
        this.vid = "-1";
        this.timestamp = "0";
        this.attributes = new HashMap<String, String>();
        this.outgoing_edges = new HashMap<String, Edge>();
        this.incoming_edges = new HashMap<String, Edge>();
    }

    public HashMap<String, Edge> getOutgoing_edges()
    {
        return outgoing_edges;
    }

    public HashMap<String, Edge> getIncoming_edges()
    {
        return incoming_edges;
    }
    
    public void setOutgoing_edges(Map<String, Edge> outEdges)
    {
        this.outgoing_edges.putAll(outEdges);
    }
    
    public void setIncoming_edges(Map<String, Edge> inEdges)
    {
        this.incoming_edges.putAll(inEdges);
    }

    public String getVid()
    {
        return vid;
    }

    public void setVid(String vid)
    {
        this.vid = vid;
    }

    public String getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(String timestamp)
    {
        this.timestamp = timestamp;
    }

    public HashMap<String, String> getAttributes()
    {
        return attributes;
    }
    
    public Set<String> getNeighbors()
    {
        return outgoing_edges.keySet();
    }

    public void setAttributes(HashMap<String, String> attributes)
    {
        this.attributes = attributes;
    }
    
    public void setValue(String attrName, String attrValue)
    {
        attributes.put(attrName, attrValue);
    }

    public void addOutgoingEdge(String vid, Edge edge)
    {
        outgoing_edges.put(vid, edge);
    }

    public void addIncomingEdge(String vid, Edge edge)
    {
        incoming_edges.put(vid, edge);
    }

    @Override
    public String toString()
    {
        return "Vertex{" + "vid=" + vid + ", timestamp=" + timestamp + ", attributes=" + attributes + ", outgoing_edges=" + outgoing_edges + ", incoming_edges=" + incoming_edges + '}';
    }
}

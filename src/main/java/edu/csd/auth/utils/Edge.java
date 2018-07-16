package edu.csd.auth.utils;

public class Edge
{
    public String label;
    public String weight;
    public String otherEnd;
    public String start;
    public String end;
    
    public Edge(String otherEnd)
    {
        this.label = null;
        this.weight = null;
        this.otherEnd = otherEnd;
        this.start = null;
        this.end = null;
    }
    
    public Edge(String label, String weight, String otherEnd, String start, String end)
    {
        this.label = label;
        this.weight = weight;
        this.otherEnd = otherEnd;
        this.start = start;
        this.end = end;
    }
    
    @Override
    public String toString()
    {
        return "{" + "label: '" + label + "', weight: '" + weight + "', otherEnd: '" + otherEnd + "', start: '" + start + "', end: '" + end + "'}";
    }
    
    public boolean existsInInterval(String first, String last)
    {
        return (Integer.parseInt(first) >= Integer.parseInt(start) && Integer.parseInt(first) < Integer.parseInt(end)) || (Integer.parseInt(last) >= Integer.parseInt(start) && Integer.parseInt(last) < Integer.parseInt(end));
    }
}

package edu.csd.auth.utils;

public class SnapshotResult implements Comparable<SnapshotResult>
{ 
    public String sID;
    public Double value;
    
    public SnapshotResult(String sID, Double value)
    {
        this.sID = sID;
        this.value = value;
    }
    
    @Override
    public int compareTo(SnapshotResult o)
    {
        return -this.value.compareTo(o.value);
    }
    
    @Override
    public String toString()
    {
        return "(" + sID + ": " + value + ")";
    }
}

package edu.csd.auth.utils;

public class Interval implements Comparable<Interval>
{
    public String value;
    public String start;
    public String end;

    public Interval(String value, String start, String end)
    {
        this.value = value;
        this.start = start;
        this.end = end;
        if (end.equalsIgnoreCase("Infinity"))
            this.end = String.valueOf(Integer.MAX_VALUE);
    }

    public Interval() 
    {
        this.value = null;
        this.start = null;
        this.end = null;
    }

    @Override
    public String toString()
    {
        return "{value:'" + value + "', start:'" + start + "', end:'" + end + "'}";
    }

    @Override
    public int compareTo(Interval o)
    {
        if (Integer.parseInt(this.end) <= Integer.parseInt(o.start))
            return -1;
        else if (Integer.parseInt(o.end) <= Integer.parseInt(this.start))
            return 1;
        else
            return 0;
    }
    
    public boolean stab(String timestamp) //timestamp \in [a,b)
    {
        return Integer.parseInt(timestamp) >= Integer.parseInt(start) && Integer.parseInt(timestamp) < Integer.parseInt(end);
    }
}

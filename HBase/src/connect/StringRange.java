package connect;

public class StringRange {
	  private String start = null;
	  private String end = null;
	  private boolean startInclusive = true;
	  private boolean endInclusive = false;

	  public StringRange(String start, boolean startInclusive, String end,
	      boolean endInclusive) {
	    this.start = start;
	    this.startInclusive = startInclusive;
	    this.end = end;
	    this.endInclusive = endInclusive;
	  }

	  public String getStart() {
	    return this.start;
	  }

	  public String getEnd() {
	    return this.end;
	  }

	  public boolean isStartInclusive() {
	    return this.startInclusive;
	  }

	  public boolean isEndInclusive() {
	    return this.endInclusive;
	  }

	  @Override
	  public int hashCode() {
	    int hashCode = 0;
	    if (this.start != null) {
	      hashCode ^= this.start.hashCode();
	    }

	    if (this.end != null) {
	      hashCode ^= this.end.hashCode();
	    }
	    return hashCode;
	  }

	  @Override
	  public String toString() {
	    String result = (this.startInclusive ? "[" : "(")
	          + (this.start == null ? null : this.start) + ", "
	          + (this.end == null ? null : this.end)
	          + (this.endInclusive ? "]" : ")");
	    return result;
	  }

	   public boolean inRange(String value) {
	    boolean afterStart = true;
	    if (this.start != null) {
	      int startCmp = value.compareTo(this.start);
	      afterStart = this.startInclusive ? startCmp >= 0 : startCmp > 0;
	    }

	    boolean beforeEnd = true;
	    if (this.end != null) {
	      int endCmp = value.compareTo(this.end);
	      beforeEnd = this.endInclusive ? endCmp <= 0 : endCmp < 0;
	    }

	    return afterStart && beforeEnd;
	  }
}

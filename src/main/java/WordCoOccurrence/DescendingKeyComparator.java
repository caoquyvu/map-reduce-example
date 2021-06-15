package WordCoOccurrence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by quyvc on 06/03/2017.
 */
public class DescendingKeyComparator extends IntWritable.Comparator {
    public DescendingKeyComparator() {
    }

    public int compare(WritableComparable a, WritableComparable b) {
        return super.compare(b, a);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return super.compare(b2, s2, l2, b1, s1, l1);
    }
}
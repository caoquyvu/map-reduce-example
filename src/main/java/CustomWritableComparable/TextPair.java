package CustomWritableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by quyvc on 06/03/2017.
 */

public class TextPair implements WritableComparable<TextPair> {

    private Text first;
    private Text second;

    public TextPair() {
        set(new Text(), new Text());
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public String toString() {
        return first + " " + second;
    }

    public int compareTo(TextPair tp) {
        int cmp = first.compareTo(tp.first);

        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

 }

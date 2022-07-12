package comp9313.proj1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class WritableProj1 implements Writable, WritableComparable<WritableProj1> {
    private String term;
    private int specialKey;
    private String year;

    public WritableProj1() {
    }

    public WritableProj1(String term, int specialKey, String year) {
        this.term = term;
        this.specialKey = specialKey;
        this.year = year;
    }

    public WritableProj1(String value, int i, String substring, IntWritable one) {
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public int getSpecialKey() {
        return specialKey;
    }

    public void setSpecialKey(int specialKey) {
        this.specialKey = specialKey;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(term);
        dataOutput.writeInt(specialKey);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        term = dataInput.readUTF();
        specialKey = dataInput.readInt();
        year = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "WritableDemo{" +
                "term='" + term + '\'' +
                ", specialKey=" + specialKey +
                ", year='" + year + '\'' +
                '}';
    }

    @Override
    public int compareTo(@NotNull WritableProj1 o) {
        int result = this.term.compareTo(o.term);
        if (result == 0){
            result = Integer.compare(this.specialKey, o.specialKey);
            if (result == 0){
                return this.year.compareTo(o.year);
            }
        }
        return result;
    }
}

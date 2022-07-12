package lab3.Secondar_Sorting;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritableDemo implements Writable, WritableComparable<WritableDemo> {
    private String yearMonth;
    private int temperature;

    public WritableDemo() {
    }

    public WritableDemo(String yearMonth, int temperature) {
        this.yearMonth = yearMonth;
        this.temperature = temperature;
    }

    public String getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth = yearMonth;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(yearMonth);
        dataOutput.writeInt(temperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        yearMonth = dataInput.readUTF();
        temperature = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "SecondarySortingWritable{" +
                "yearMonth='" + yearMonth + '\'' +
                ", temperature=" + temperature +
                '}';
    }

    @Override
    public int compareTo(@NotNull WritableDemo o) {
        int result = this.yearMonth.compareTo(o.getYearMonth());
        if (result == 0){
            return Integer.compare(this.temperature, o.getTemperature());
        }
        return result;
    }
}

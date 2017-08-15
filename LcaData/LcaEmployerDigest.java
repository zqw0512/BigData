package LcaData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class LcaEmployerDigest implements Writable {
	private String companyName;
	private String title;
	private String state;
	private int salaryFrom = 0;
	private int salaryTo = 0;
	private int salaryAverage = 0;

	public String getCompanyName() {
		return companyName;
	}

	public String getTitle() {
		return title;
	}

	public String getState() {
		return state;
	}

	public int getSalaryFrom() {
		return salaryFrom;
	}

	public int getSalaryTo() {
		return salaryTo;
	}

	public int getSalaryAverage() {
		return salaryAverage;
	}

	public int getCount() {
		return count;
	}

	private int count = 0;

	public void readFields(DataInput in) throws IOException {
		companyName = (String) in.readUTF();
		title = (String) in.readUTF();
		state = (String) in.readUTF();
		salaryFrom = new Integer((int) in.readInt());
		salaryTo = new Integer((int) in.readInt());
		salaryAverage = new Integer((int) in.readInt());
		count = new Integer((int) in.readInt());
	}

	public void write(DataOutput out) throws IOException {
		System.out.println("writing for :" + companyName + ":" + state + ":"
				+ title + ":" + salaryFrom + ":" + salaryTo);
		try {
			out.writeUTF(companyName);
			out.writeUTF(title);
			out.writeUTF(state);
			out.writeInt(salaryFrom);
			out.writeInt(salaryTo);
			out.writeInt(salaryAverage);
			out.writeInt(count);
		} catch (Exception e) {
			System.out.println("write failed for :" + companyName + ":" + state
					+ ":" + title + ":" + salaryFrom + ":" + salaryTo + ","
					+ count);

		}
	}

	@Override
	public String toString() {
		return ";;" + salaryAverage + ";;" + title + ";;" + state + ";;"
				+ count;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public void setState(String state) {
		this.state = state;
	}

	public void setSalaryFrom(int salaryFrom) {
		this.salaryFrom = salaryFrom;
	}

	public void setSalaryTo(int salaryTo) {
		this.salaryTo = salaryTo;
	}

	public void setSalaryAverage(int salaryAverage) {
		this.salaryAverage = salaryAverage;
	}

	public void setCount(int count) {
		this.count = count;
	}

	// public String toString() {
	// return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
	// }
}

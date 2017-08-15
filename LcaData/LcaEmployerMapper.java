package LcaData;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//bin/hadoop fs -cat assignment3/14/*| sort -t \| -nk 3
public class LcaEmployerMapper extends
		Mapper<Object, Text, Text, LcaEmployerDigest> {

	public static int STATUS = 1;// "CERTIFIED"
	public static int COMPANY_NAME = 7;
	public static int COMPANY_CITY = 9;
	public static int COMPANY_STATE = 10;
	public static int COMPANY_ZIP = 11;
	public static int JOB_TITLE = 19;
	public static int SOC_NAME = 21;
	public static int COUNT = 23;
	public static int PW = 25;
	public static int PW_UNIT = 26;// "Year", "Hour","Week","Month","Bi-Weekly"
	public static int WAGE_FROM = 30;
	public static int WAGE_TO = 31;
	public static int WAGE_UNIT = 32;

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Text name = new Text();

		String[] line = value.toString().split(
				",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

		if (isCertified(line)) {
			String companyName = line[COMPANY_NAME];
			String title = line[JOB_TITLE];
			String state = line[COMPANY_STATE];
			if (companyName != null && title != null && state != null) {

				name.set(companyName);
				System.out.println("line is " + value + ", name is "
						+ line[COMPANY_NAME] + "," + line[JOB_TITLE]);

				try {
					String strFrom = line[WAGE_FROM];
					String strTo = line[WAGE_TO];
					if (line[WAGE_FROM].contains("-")) {
						String[] ss = (strFrom + " ").split("-");
						strFrom = ss[0];
						strTo = ss[1];
					}
					int salaryFrom = calcSalary(strFrom, line[WAGE_UNIT]);
					int salaryTo = calcSalary(strTo, line[WAGE_UNIT]);
					if (salaryTo < salaryFrom) {
					} else {
						salaryTo = salaryFrom;
					}

					LcaEmployerDigest digest = new LcaEmployerDigest();
					digest.setCompanyName(companyName.replaceAll("\"", ""));
					digest.setTitle(title.replaceAll("\"", ""));
					digest.setState(state);
					digest.setSalaryFrom(salaryFrom);
					digest.setSalaryTo(salaryTo);
					digest.setSalaryAverage(0);
					digest.setCount(getInt(line[COUNT]));
					context.write(name, digest);

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				System.out.println("!!! empty name or title" + value);
			}
		}
	}

	static private boolean isCertified(String[] line) {
		return line != null && line.length > STATUS
				&& "CERTIFIED".equalsIgnoreCase(line[STATUS]);
	}

	static private int calcSalary(String val, String unit) {
		Integer v = parseCurrency(val);
		if ("Year".equalsIgnoreCase(unit)) {
			return v;
		} else if ("Month".equalsIgnoreCase(unit)) {
			return v * 12;
		} else if ("Week".equalsIgnoreCase(unit)) {
			return v * 52;
		} else if ("Bi-Weekly".equalsIgnoreCase(unit)) {
			return v * 26;
		} else if ("Hour".equalsIgnoreCase(unit)) {
			return v * 2200;
		} else {
			System.out.println("!!!invalid salary unit:" + unit);
			return 0;
		}

	}

	static private int getInt(String s) {
		try {
			return (int) Float.parseFloat(s);
		} catch (Exception e) {
			return -1;
		}

	}

	static private Integer parseCurrency(String c) {
		String newStr = c.replaceAll("[^\\d.]+", "");
		return getInt(newStr);

	}

	public static void main(String[] args) {
		String s = "I-200-09146-796321,CERTIFIED,12/12/2014,12/18/2014,H-1B,01/05/2015,01/04/2018,UNIVERSITY OF OKLAHOMA,905 ASP AVE,NORMAN,OK,73019,UNITED STATES OF AMERICA,,4053251826,,,,,ASSISTANT PROFESSOR,25-1032,\"ENGINEERING TEACHERS, POSTSECONDARY\",611310,1,Y,42860.00,Year,Other,2014,OFLC ONLINE DATA CENTER,85000.00 -,,Year,N,N,NORMAN,CLEVELAND,OK,73019";
		String[] aa = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		for (int i = 0; i < aa.length; i++) {
			System.out.println(i + ":" + aa[i]);
		}
		(aa[30] + " ").split("-");

		System.out.println(aa[30] + ":" + aa[31]);
		System.out.println("From:" + calcSalary(aa[30].split("-")[0], aa[32]));
		System.out.println("To:" + calcSalary(aa[30].split("-")[1], aa[32]));
		System.out.println(aa[7].replaceAll("\"", "") + ":" + aa[19] + ":"
				+ aa[23]);
	}
}

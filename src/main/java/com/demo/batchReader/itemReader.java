package com.demo.batchReader;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

public class itemReader implements Tasklet {
	@Autowired
	public  Environment env;

	
	static PreparedStatement stmt = null;

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

		String csvFilePath = "C:\\Peter\\Job/Rock.csv";
		this.Conversion(csvFilePath);
//		Connection con = con();
//		BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
//		String line;
//		try {
//			while ((line = reader.readLine()) != null) {
//				String[] data = line.split(",");
//				int id = Integer.parseInt(data[0]);
//				String name = data[1];
//				int maths = Integer.parseInt(data[2]);
//				int physics = Integer.parseInt(data[3]);
//				int chemistry = Integer.parseInt(data[4]);
//				int total = Integer.parseInt(data[5]);
//				StringBuilder insertQuery = new StringBuilder(
//						"INSERT INTO ROCK (id ,name,maths,physics,chemistry,total) VALUES (?,?,?,?,?,?)");
//				stmt = con.prepareStatement(insertQuery.toString());
//				stmt.setInt(1, id);
//				stmt.setString(2, name);
//				stmt.setInt(3, maths);
//				stmt.setInt(4, physics);
//				stmt.setInt(5, chemistry);
//				stmt.setInt(6, total);
//				int rs = stmt.executeUpdate();
//			}
//		} catch (Exception e) {
//			System.out.println(e);
//		} finally {
//			if (reader != null) {
//				reader.close();
//			}
//		}
//
//		System.out.println("Data Inserted succefully ");
//
		return RepeatStatus.FINISHED;

	}

	public Connection con() {

		Connection con = null;
		String url = env.getProperty("spring.datasource.url"); // Use the correct property key
		String username = env.getProperty("spring.datasource.username"); // Use the correct property key
		String password = env.getProperty("spring.datasource.password"); // Use the correct property key
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return con;
	}

	public void Conversion(String csvfile) {
		List<Map<String, String>> dataRows = new ArrayList<>();

//		try (BufferedReader br = new BufferedReader(new FileReader(csvfile))) {
//
//			String line;
//			while ((line = br.readLine()) != null) {
//				String[] values = line.split(",");
//				String header = values[0].trim();
//				String value = values[1].trim();
//				HashMap<String, String> dataRow = new HashMap<>();
//				dataRow.put(header, value);
//				dataRows.add(dataRow);
//				System.out.println(dataRows);
//			}
//
//			// Print the list of hashmaps
//			for (HashMap<String, String> dataRow : dataRows) {
//				System.out.println(dataRow);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		Map<String, String> dataMap = new HashMap<>();
		List<String> insertQueries = new ArrayList<>();
		try {
			Connection con = con();
			Reader read = new FileReader(csvfile);
			CSVParser csvParser = new CSVParser(read,
					CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase());
			for (CSVRecord csvRecord : csvParser) {
				StringBuilder queryBuilder = new StringBuilder(
						"INSERT INTO Rock (ID, NAME, MATHS, PHYSICS, CHEMISTRY, TOTAL) VALUES (");
				queryBuilder.append(csvRecord.get("ID")).append(", ");
				queryBuilder.append("'").append(csvRecord.get("NAME")).append("', ");
				queryBuilder.append(csvRecord.get("MATHS")).append(", ");
				queryBuilder.append(csvRecord.get("PHYSICS")).append(", ");
				queryBuilder.append(csvRecord.get("CHEMISTRY")).append(", ");
				queryBuilder.append(csvRecord.get("TOTAL"));
				queryBuilder.append(")");
				insertQueries.add(queryBuilder.toString());

			}

			Statement stmt = con.createStatement();

			for (String query : insertQueries) {
				stmt.executeUpdate(query);
				System.out.println(query);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
/*
 * dataMap.put("ID", csvRecord.get("ID")); dataMap.put("Name",
 * csvRecord.get("NAME")); dataMap.put("Maths", csvRecord.get("MATHS"));
 * dataMap.put("Physics", csvRecord.get("PHYSICS")); dataMap.put("Chemistry",
 * csvRecord.get("CHEMISTRY")); dataMap.put("Total", csvRecord.get("TOTAL"));
 * dataRows.add(dataMap);
 */

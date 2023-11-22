package com.demo.batchReader;

import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

public class Reader implements Tasklet {
	@Autowired
	Environment env;

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		String folder = env.getProperty("spring.datasource.path");
		File file = new File(folder);
		File[] listfiles = file.listFiles();
		List<File> csvFiles = new ArrayList<>();
		Connection con = con();

		for (File files : listfiles) {
			if (files.isFile()) {
				String fileName = files.getName();
				String sql = "Select COUNT(*) as count from Rock where FILENAME ='" + fileName + "'";
				PreparedStatement stmt = con.prepareStatement(sql);
				ResultSet response = stmt.executeQuery();
				BigDecimal count = new BigDecimal(0);

				while (response.next())
					count = response.getBigDecimal("COUNT");

				int intValue = count.intValue();
				if (intValue <= 0 && (fileName.endsWith(".csv"))) {
					csvFiles.add(files);
				} else {
					System.out.println("File Is Alread Read And Stored in DataBase");
				}
			}
		}
		List<Map<String, String>> Readvalues = new ArrayList<>();
		for (File csvFile : csvFiles) {
			List<Map<String, String>> values = readCSVData(csvFile);
			Readvalues.addAll(values);
		}
//		System.out.println(Readvalues);
		List<String> insertQueries = buildInsertQueries(Readvalues);

//		executeInsertQueries(con, insertQueries);
		for (String query : insertQueries) {
			Statement stmt = con.createStatement();
			stmt.executeUpdate(query);
			System.out.println(query);
			System.out.println("Data Inserted Successfully");
		}

		return RepeatStatus.FINISHED;
	}

	public List<String> buildInsertQueries(List<Map<String, String>> readvalues) {
		List<String> insertQueries = new ArrayList<>();
		for (Map<String, String> recordMap : readvalues) {
			StringBuilder queryBuilder = new StringBuilder(
					"INSERT INTO Rock (ID, NAME, MATHS, PHYSICS, CHEMISTRY, TOTAL,FILENAME) VALUES (");
			queryBuilder.append(recordMap.get("ID")).append(", ");
			queryBuilder.append("'").append(recordMap.get("NAME")).append("', ");
			queryBuilder.append(recordMap.get("MATHS")).append(", ");
			queryBuilder.append(recordMap.get("PHYSICS")).append(", ");
			queryBuilder.append(recordMap.get("CHEMISTRY")).append(", ");
			queryBuilder.append(recordMap.get("TOTAL")).append(", ");
			queryBuilder.append("'").append(recordMap.get("FILENAME")).append("')");
			insertQueries.add(queryBuilder.toString());
		}
		return insertQueries;

	}

	public List<Map<String, String>> readCSVData(File file) {
		List<Map<String, String>> Data = new ArrayList<>();
		try {
			FileReader reader = new FileReader(file.getAbsolutePath());
			CSVParser parse = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
			for (CSVRecord record : parse) {
				Map<String, String> recordMap = new HashMap<>();
				recordMap.put("ID", record.get("ID"));
				String name = record.get("NAME");
				Connection con = con();
				String sql = "Select * from Rock where NAME =  '" + name + "'";
				PreparedStatement prepare = con.prepareStatement(sql);
				ResultSet response = prepare.executeQuery();
				BigDecimal count = new BigDecimal(0);

				while (response.next())
					count = response.getBigDecimal("COUNT");

				int intValue = count.intValue();
				if (intValue <= 0) {
					recordMap.put("NAME", record.get("NAME"));
				} else {
					System.out.println("Name Is Already Stored ");
				}
				recordMap.put("MATHS", record.get("MATHS"));
				recordMap.put("PHYSICS", record.get("PHYSICS"));
				recordMap.put("CHEMISTRY", record.get("CHEMISTRY"));
				recordMap.put("TOTAL", record.get("TOTAL"));
				recordMap.put("FILENAME", file.getName());
				Data.add(recordMap);
			}
			parse.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return Data;
	}

	public Connection con() {

		Connection con = null;
		String url = env.getProperty("spring.datasource.url");
		String username = env.getProperty("spring.datasource.username");
		String password = env.getProperty("spring.datasource.password");
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return con;
	}

}

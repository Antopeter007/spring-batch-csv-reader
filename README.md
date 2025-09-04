# 📂 Spring Batch CSV Reader

This project demonstrates how to read **CSV files** and insert the data into a database using **Spring Batch** and **Apache Commons CSV**.

---

## ▶️ How It Works

1. Reads data from a CSV file (e.g., `Rock.csv`) using **CSVParser**.  
2. Converts each CSV record into an **INSERT SQL statement**.  
3. Inserts the records into the database table `Rock` using JDBC.  
4. Supports database connection using **Spring Environment properties** for URL, username, and password.

---

## 🛠 Features

- Batch reading of CSV files.  
- Inserts data into **Oracle database** (can be adapted for other DBs).  
- Uses **Spring Batch Tasklet** to execute the job.  
- Handles headers automatically with **CSVFormat.DEFAULT.withFirstRecordAsHeader()**.

---

## 📂 CSV File Structure Example

```csv
ID,NAME,MATHS,PHYSICS,CHEMISTRY,TOTAL
1,Anto Peter,90,85,95,270
2,Arun Smith,80,70,90,240
```
## 🏃 How to Run

- Update the CSV file path in itemReader.java.

- Ensure the database table Rock exists with the appropriate columns.

- Configure Spring Environment properties with DB URL, username, and password.

- Run the Spring Batch job to process the CSV and insert data.


## 🛠 Tech Stack

- Java 17+

- Spring Boot

- Spring Batch

- Apache Commons CSV

- Oracle / MySQL / any JDBC-supported database

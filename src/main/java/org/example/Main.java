package org.example;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        List<String> listOfPeople = new ArrayList<>();

        try {
            XSSFWorkbook workbook = (XSSFWorkbook) WorkbookFactory.create(new FileInputStream("sample/data1000.xlsx"));
            Sheet sheetAlunos = workbook.getSheetAt(0);

            for (Row row : sheetAlunos) {
                Iterator<Cell> cellIterator = row.cellIterator();

                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    if (cell.getColumnIndex() == 0) {
                        listOfPeople.add(cell.getStringCellValue());
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();


            System.out.println("File not found");
        }

        System.out.println("First record from sample: " + listOfPeople.get(0));
        System.out.println("Last record from sample: " + (listOfPeople.get(listOfPeople.size()-1)));

        synchronousProcessing(listOfPeople);
        parallelProcessing(listOfPeople);
    }

    static void synchronousProcessing(List<String> listOfPeople) throws SQLException {
        DbConnection.cleanDatabase();
        long startTime = System.currentTimeMillis();
        listOfPeople.forEach(person -> {
            try {
                DbConnection.insertPerson(person);
            }catch (Exception e) {
                System.out.println(e.getMessage());
            }
        });
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println("[Java] Synchronous implementation took " + elapsedTime + " milliseconds." );
        System.out.println("[Java] Processed " + listOfPeople.size() + " records.");
    }



    static void parallelProcessing(List<String> listOfPeople) throws SQLException {
        DbConnection.cleanDatabase();
        long startTime = System.currentTimeMillis();
        listOfPeople.stream().parallel().forEach(person -> {
            try {
                DbConnection.insertPerson(person);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println("[Java] Parallel implementation took " + elapsedTime + " milliseconds." );
        System.out.println("[Java] Processed " + listOfPeople.size() + " records.");
    }
}
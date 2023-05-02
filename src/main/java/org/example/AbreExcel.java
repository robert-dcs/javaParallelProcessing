package org.example;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class AbreExcel {

    public static void main(String[] args) throws IOException, SQLException, ExecutionException, InterruptedException {
        System.out.println(Thread.activeCount());
        System.out.println(Runtime.getRuntime().availableProcessors());

        List<String> listOfPerson = new ArrayList<>();

        try {
            XSSFWorkbook workbook = (XSSFWorkbook) WorkbookFactory.create(new FileInputStream("data.xlsx"));
            Sheet sheetAlunos = workbook.getSheetAt(0);

            Iterator<Row> rowIterator = sheetAlunos.iterator();

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                Iterator<Cell> cellIterator = row.cellIterator();
                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    switch (cell.getColumnIndex()) {
                        case 0:
                            listOfPerson.add(cell.getStringCellValue());
                            break;
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Arquivo Excel não encontrado!");
        }

        if (listOfPerson.size() == 0) {
            System.out.println("Nenhuma pessoa encontrado!");
        } else {
            System.out.println("Tamanho ta lista: " + listOfPerson.size());
            System.out.println("Primeira pessoa: " + listOfPerson.get(0));
            System.out.println("Ultima pessoa: " + listOfPerson.get(listOfPerson.size() - 1));
        }

        synchronousProcessing(listOfPerson);
        parallelProcessing(listOfPerson);
    }

    static void synchronousProcessing(List<String> people) throws SQLException {
        DbConnection.cleanDatabase(DbConnection.connect());
        long startTime = System.nanoTime();
        people.forEach(person -> {
            try {
                DbConnection.insertPeople(person, DbConnection.connect());
            }catch (Exception e) {
                System.out.println(e.getMessage());
            }
        });
        long endTime = System.nanoTime();
        long elapsedTime = startTime - endTime;
        double elapsedTimeInSecond = (double) (elapsedTime / 1_000_000_000)*(-1);
        String message = "Time duration: " + elapsedTimeInSecond + " seconds";
        System.out.println("Finished synchronous saving. " + message);
    }



    static void parallelProcessing(List<String> people) throws SQLException, ExecutionException, InterruptedException {
        DbConnection.cleanDatabase(DbConnection.connect());
        long startTime = System.nanoTime();

        people.stream().parallel().forEach(person -> {
            try {
                DbConnection.insertPeople(person, DbConnection.connect());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        long endTime = System.nanoTime();
        long elapsedTime = startTime - endTime;
        double elapsedTimeInSecond = (double) elapsedTime / 1_000_000_000*(-1);
        String message = "Time duration: " + elapsedTimeInSecond + " seconds";
        System.out.println("Finished parallel saving. " + message);
    }
}
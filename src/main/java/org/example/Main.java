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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

public class Main {
    public static void main(String[] args) throws IOException, SQLException {

        for (int sampleSize = 1000; sampleSize <= 1000000; sampleSize *= 10) {
            System.out.println("\n*--------- New execution: sample size " + sampleSize + " ----------*");
            List<String> listOfPeople = new ArrayList<>();
            try {
                XSSFWorkbook workbook = (XSSFWorkbook) WorkbookFactory.create(new FileInputStream("sample/data"+ sampleSize +".xlsx"));
                Sheet sheet = workbook.getSheetAt(0);

                for (Row row : sheet) {
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
            System.out.println("Last record from sample: " + (listOfPeople.get(listOfPeople.size() - 1)));

            for (int idx=0; idx < 3; idx++) {
                System.out.println("*------------"
                        + idx + " execution ------------*");
                synchronousProcessing(listOfPeople);
                parallelProcessing(listOfPeople);
                parallelProcessing2(listOfPeople);
                parallelProcessing3(listOfPeople);
            }
        }
    }

    static void synchronousProcessing(List<String> listOfPeople) throws SQLException {
        System.out.println("\n*--------- Synchronous ----------*");

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
        DbConnection.countRows();
        DbConnection.getFirstRecord();
        DbConnection.getLastRecord();
    }



    static void parallelProcessing(List<String> listOfPeople) throws SQLException {
        System.out.println("\n*--------- Parallel 1 ----------*");

        DbConnection.cleanDatabase();
        long startTime = System.currentTimeMillis();
        listOfPeople.parallelStream().forEach(person -> {
            try {
                DbConnection.insertPerson(person);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println("[Java] Parallel implementation 1 took " + elapsedTime + " milliseconds." );
        System.out.println("[Java] Processed " + listOfPeople.size() + " records.");
        DbConnection.countRows();
        DbConnection.getFirstRecord();
        DbConnection.getLastRecord();
    }

    static void parallelProcessing2(List<String> listOfPeople) throws SQLException {
        System.out.println("\n*--------- Parallel 2 ----------*");

        DbConnection.cleanDatabase();
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        long startTime = System.currentTimeMillis();
        try {
            List<CompletableFuture<Void>> futures = listOfPeople.stream()
                    .map(person -> CompletableFuture.runAsync(() -> {
                        try {
                            DbConnection.insertPerson(person);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }, executor))
                    .toList();

            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println("[Java] Parallel implementation 2 took " + elapsedTime + " milliseconds." );
        System.out.println("[Java] Processed " + listOfPeople.size() + " records.");
        DbConnection.countRows();
        DbConnection.getFirstRecord();
        DbConnection.getLastRecord();
    }

    static void parallelProcessing3(List<String> listOfPeople) throws SQLException {
        System.out.println("\n*--------- Parallel 3 ----------*");

        DbConnection.cleanDatabase();
        long startTime = System.currentTimeMillis();
        ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();
        forkJoinPool.invoke(new  ParallelProcessor(listOfPeople));
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println("[Java] Parallel implementation 3 took " + elapsedTime + " milliseconds." );
        System.out.println("[Java] Processed " + listOfPeople.size() + " records.");
        DbConnection.countRows();
        DbConnection.getFirstRecord();
        DbConnection.getLastRecord();
    }
}

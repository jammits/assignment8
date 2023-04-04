package com.coderscampus.assignment.com.coderscampus.assignment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Assignment8 {
    private List<Integer> numbers = null;
    private AtomicInteger i = new AtomicInteger(0);

    public Assignment8() {
        try {
            // Make sure you download the output.txt file for Assignment 8
            // and place the file in the root of your Java project
            numbers = Files.readAllLines(Paths.get("com/coderscampus/assignment/output.txt"))
                    .stream()
                    .map(n -> Integer.parseInt(n))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method will return the numbers that you'll need to process from the list
     * of Integers. However, it can only return 1000 records at a time. You will
     * need to call this method 1,000 times in order to retrieve all 1,000,000
     * numbers from the list
     * 
     * @return Integers from the parsed txt file, 1,000 numbers at a time
     */
    public List<Integer> getNumbers() {
        int start, end;
        synchronized (i) {
            start = i.get();
            end = i.addAndGet(1000);

            System.out.println("Starting to fetch records " + start + " to " + (end));
        }
        // force thread to pause for half a second to simulate actual Http / API traffic
        // delay
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }

        List<Integer> newList = new ArrayList<>();
        IntStream.range(start, end)
                .forEach(n -> {
                    newList.add(numbers.get(n));
                });
        System.out.println("Done Fetching records " + start + " to " + (end));
        return newList;
    }

    public static void updateValues(Integer number, ConcurrentMap<Integer,Integer> seenNumbers) {
        synchronized (seenNumbers) {
            if (seenNumbers.containsKey(number)) {
                Integer incrementValue = seenNumbers.get(number);
                incrementValue++;
                seenNumbers.put(number,incrementValue);
            }
        }

    }

    public static void main(String[] args) {
        Assignment8 assignment = new Assignment8();

        ExecutorService fetchPool = Executors.newCachedThreadPool();
        ConcurrentMap<Integer,Integer> seenNumbers = new ConcurrentHashMap<>();

        //Setting keys of the map for unique instances of a number's occurrence
        for (Integer i = 0; i < 15; i++) {
            seenNumbers.put(i, 0);
        }
        List<CompletableFuture<Void>> tasks = new ArrayList<>();



        //Assigning values to the hashmap for identifying unique records
        for (int i = 0; i < 1000; i++) {
            CompletableFuture<Void> task = CompletableFuture.supplyAsync(assignment::getNumbers, fetchPool)
                    .thenAcceptAsync(numbers -> {
                         numbers.stream().forEach(number -> {updateValues(number,seenNumbers);
                         });
                    }, fetchPool);

            tasks.add(task);
        }

        //Waiting for all threads to complete using allOf
        CompletableFuture<Void> combinedFutures = CompletableFuture.allOf(tasks.toArray(new CompletableFuture[tasks.size()]));
        //Waiting for combineFutures to finish waiting on all threads
        combinedFutures.join();

        seenNumbers.entrySet().stream().forEach(group -> System.out.println(group));


    }

}
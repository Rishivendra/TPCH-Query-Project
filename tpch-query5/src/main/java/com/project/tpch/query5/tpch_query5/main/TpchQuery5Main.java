package com.project.tpch.query5.tpch_query5.main;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.project.tpch.query5.tpch_query5.dataloader.FileDataLoader;
import com.project.tpch.query5.tpch_query5.model.Customer;
import com.project.tpch.query5.tpch_query5.model.LineItem;
import com.project.tpch.query5.tpch_query5.model.Nation;
import com.project.tpch.query5.tpch_query5.model.Order;
import com.project.tpch.query5.tpch_query5.model.Region;
import com.project.tpch.query5.tpch_query5.model.Supplier;

public class TpchQuery5Main {
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      args = new String[]{
          "--r_name", "ASIA",
          "--start_date", "1994-01-01",
          "--end_date", "1995-01-01",
          "--threads", "4",
          "--table_path", "C:/Users/Rishivendra Gupta/Documents/tpch-query5/tpch-dbgen",
          "--result_path", "C:/Users/Rishivendra Gupta/Documents/tpch-query5/results"
      };
    }
    if (args.length != 12) {
      System.out.println(
          "Usage: java App --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 1 --table_path path --result_path path");
      return;
    }

    String region = null;
    LocalDate start = null;
    LocalDate end = null;
    int threads = 1;
    String tablePath = null;
    String resultPath = null;

    for (int i = 0; i < args.length; i += 2) {
      switch (args[i]) {
        case "--r_name" -> region = args[i + 1];
        case "--start_date" -> start = LocalDate.parse(args[i + 1]);
        case "--end_date" -> end = LocalDate.parse(args[i + 1]);
        case "--threads" -> threads = Integer.parseInt(args[i + 1]);
        case "--table_path" -> tablePath = args[i + 1];
        case "--result_path" -> resultPath = args[i + 1];
        default -> {
          System.out.println("Unknown argument: " + args[i]);
          return;
        }
      }
    }

    if (region == null || start == null || end == null || tablePath == null || resultPath == null) {
      System.out.println("Missing required arguments.");
      return;
    }

    FileDataLoader loader = new FileDataLoader();
    List<Customer> customers = loader.loadCustomers(tablePath);
    List<Order> orders = loader.loadOrders(tablePath);
    List<LineItem> lineItems = loader.loadLineItems(tablePath);
    List<Supplier> suppliers = loader.loadSuppliers(tablePath);
    List<Nation> nations = loader.loadNations(tablePath);
    List<Region> regions = loader.loadRegions(tablePath);

    String finalRegion = region;
    Set<Integer> asiaRegionKeys = regions.stream()
        .filter(r -> r.getName().equals(finalRegion))
        .map(Region::getRegionKey).collect(Collectors.toSet());

    Set<Integer> asiaNationKeys = nations.stream()
        .filter(n -> asiaRegionKeys.contains(n.getRegionKey()))
        .map(Nation::getNationKey).collect(Collectors.toSet());

    Set<Integer> asiaSuppKeys = suppliers.stream()
        .filter(s -> asiaNationKeys.contains(s.getNationKey()))
        .map(Supplier::getSuppKey).collect(Collectors.toSet());

    Map<Integer, String> nationKeyToName = nations.stream()
        .collect(Collectors.toMap(Nation::getNationKey, Nation::getName));

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Future<Map<String, Double>>> futures = new ArrayList<>();
    int batchSize = orders.size() / threads;

    for (int i = 0; i < threads; i++) {
      int from = i * batchSize;
      int to = (i == threads - 1) ? orders.size() : (i + 1) * batchSize;
      List<Order> orderSlice = orders.subList(from, to);

      LocalDate finalStart = start;
      LocalDate finalEnd = end;
      futures.add(executor.submit(() -> {
        Map<String, Double> localRevenue = new HashMap<>();
        for (Order o : orderSlice) {
          if (o.getOrderDate().isBefore(finalStart) || !o.getOrderDate().isBefore(finalEnd)) continue;
          Customer cust = customers.stream().filter(c -> c.getCustKey() == o.getCustKey()).findFirst().orElse(null);
          if (cust == null || !asiaNationKeys.contains(cust.getNationKey())) continue;
          for (LineItem li : lineItems.stream().filter(li -> li.getOrderKey() == o.getOrderKey()).toList()) {
            if (!asiaSuppKeys.contains(li.getSuppKey())) continue;
            String nation = nationKeyToName.get(cust.getNationKey());
            localRevenue.merge(nation, li.getRevenue(), Double::sum);
          }
        }
        return localRevenue;
      }));
    }

    Map<String, Double> totalRevenue = new HashMap<>();
    for (Future<Map<String, Double>> future : futures) {
      Map<String, Double> partial = future.get();
      for (var e : partial.entrySet()) {
        totalRevenue.merge(e.getKey(), e.getValue(), Double::sum);
      }
    }
    executor.shutdown();

    List<Map.Entry<String, Double>> sorted = totalRevenue.entrySet().stream()
        .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
        .toList();

    File resultDir = new File(resultPath);
    if (!resultDir.exists()) {
      resultDir.mkdirs();
    }


    try (PrintWriter writer = new PrintWriter(new FileWriter(resultPath + "/query5_result.csv"))) {
      writer.println("nation,revenue");
      for (var e : sorted) {
        writer.printf("%s,%.2f\n", e.getKey(), e.getValue());
      }
    }

  }
}
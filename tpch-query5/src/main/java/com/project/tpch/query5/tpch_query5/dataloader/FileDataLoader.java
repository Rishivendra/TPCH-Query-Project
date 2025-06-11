package com.project.tpch.query5.tpch_query5.dataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

import com.project.tpch.query5.tpch_query5.model.Customer;
import com.project.tpch.query5.tpch_query5.model.LineItem;
import com.project.tpch.query5.tpch_query5.model.Nation;
import com.project.tpch.query5.tpch_query5.model.Order;
import com.project.tpch.query5.tpch_query5.model.Region;
import com.project.tpch.query5.tpch_query5.model.Supplier;

public class FileDataLoader {
  public List<Customer> loadCustomers(String path) throws IOException {
    return Files.lines(Paths.get(path + "/customer.tbl"))
        .map(line -> line.split("\\|"))
        .map(p -> new Customer(Integer.parseInt(p[0]), Integer.parseInt(p[3])))
        .collect(Collectors.toList());
  }

  public List<Order> loadOrders(String path) throws IOException {
    return Files.lines(Paths.get(path + "/orders.tbl"))
        .map(line -> line.split("\\|"))
        .map(p -> new Order(Integer.parseInt(p[0]), Integer.parseInt(p[1]), LocalDate.parse(p[4])))
        .collect(Collectors.toList());
  }

  public List<LineItem> loadLineItems(String path) throws IOException {
    return Files.lines(Paths.get(path + "/lineitem.tbl"))
        .map(line -> line.split("\\|"))
        .map(p -> new LineItem(Integer.parseInt(p[0]), Integer.parseInt(p[2]), Double.parseDouble(p[5]),
            Double.parseDouble(p[6])))
        .collect(Collectors.toList());
  }

  public List<Supplier> loadSuppliers(String path) throws IOException {
    return Files.lines(Paths.get(path + "/supplier.tbl"))
        .map(line -> line.split("\\|"))
        .map(p -> new Supplier(Integer.parseInt(p[0]), Integer.parseInt(p[3])))
        .collect(Collectors.toList());
  }

  public List<Nation> loadNations(String path) throws IOException {
    return Files.lines(Paths.get(path + "/nation.tbl"))
        .map(line -> line.split("\\|"))
        .map(p -> new Nation(Integer.parseInt(p[0]), p[1], Integer.parseInt(p[2])))
        .collect(Collectors.toList());
  }

  public List<Region> loadRegions(String path) throws IOException {
    return Files.lines(Paths.get(path + "/region.tbl"))
        .map(line -> line.split("\\|"))
        .map(p -> new Region(Integer.parseInt(p[0]), p[1]))
        .collect(Collectors.toList());
  }
}

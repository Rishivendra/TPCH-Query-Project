package com.project.tpch.query5.tpch_query5.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class LineItem {
  private int orderKey;
  private int suppKey;
  private double extendedPrice;
  private double discount;

  public double getRevenue() {
    return extendedPrice * (1 - discount);
  }
}

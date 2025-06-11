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
public class Nation {
  private int nationKey;
  private String name;
  private int regionKey;
}

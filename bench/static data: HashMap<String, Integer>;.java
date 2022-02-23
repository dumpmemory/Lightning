static List<HashMap<String, Integer>> data;

public static void main(String[] args) {
  var filtered = data.stream().filter(|row| row[l_shipdate] <= data).collect(Collectors.toList());
  HashMap<(Char, Char), HashMap<String, Integer>> grouped = new HashMap<>();
  for row in filtered {
    var tuple = (row[l_returnflag], row[l_linestatus]);
    grouped.put(tuple, row);
  }
  var result = new ArrayList<>();
  for group in grouped {
    var k = group.key();
    var v = group.value();
    var l_returnflag = k[0];
    var l_linestatus = k[1];
    var avg_qty = v.stream().map(|row| row[l_quantity]).avg();
    var count_order = v.stream().count();
  }
}


// select
//         l_returnflag,
//         l_linestatus,
//         sum(l_quantity) as sum_qty,
//         sum(l_extendedprice) as sum_base_price,
//         sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
//         sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
//         avg(l_quantity) as avg_qty,
//         avg(l_extendedprice) as avg_price,
//         avg(l_discount) as avg_disc,
//         count(*) as count_order
// from
//         lineitem
// where
//         l_shipdate <= date '1998-12-01' - interval '90' day
// group by
//         l_returnflag,
//         l_linestatus
// order by
//         l_returnflag,
//         l_linestatus
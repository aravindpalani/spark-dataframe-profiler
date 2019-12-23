# spark-dataframe-profiler

A simple tool to determine column level profiling.

###### Run the spark shell with profiler jar.

`./spark-shell --jars ../profiler_2.11-0.1.jar`

###### To determine patterns for each column in your dataframe

`import org.github.aravindpalani.spark.profiler.core.pattern._`

`showPattern(df).foreach(df => df.show(false))`

###### Sample Results:

```
+----------+------+---------+----------+                                        
|CustomerID|length|frequency|percentage|
+----------+------+---------+----------+
|99999     |5     |406829   |75.07     |
|NULL      |0     |135080   |24.93     |
+----------+------+---------+----------+

```
```
+--------------------+------+---------+----------+                              
|Country             |length|frequency|percentage|
+--------------------+------+---------+----------+
|Xxxxxx Xxxxxxx      |14    |495478   |91.43     |
|Xxxxxxx             |7     |13295    |2.45      |
|Xxxxxx              |6     |11694    |2.16      |
|XXXX                |4     |8196     |1.51      |
|Xxxxxxxxxxx         |11    |4819     |0.89      |
|Xxxxx               |5     |3821     |0.71      |
|Xxxxxxxxx           |9     |1523     |0.28      |
|Xxxxxxxx            |8     |1519     |0.28      |
|Xxxxxxx Xxxxxxx     |15    |758      |0.14      |
|XXX                 |3     |349      |0.06      |
|Xxxx Xxxx           |9     |288      |0.05      |
|Xxxxxx Xxxx Xxxxxxxx|20    |68       |0.01      |
|Xxxxxxxx Xxxxxxxxx  |18    |61       |0.01      |
|Xxxxx Xxxxxxxx      |14    |30       |0.01      |
|Xxxxx Xxxxxx        |12    |10       |0.0       |
+--------------------+------+---------+----------+

```

###### To determine values for each column in your dataframe

`import org.github.aravindpalani.spark.profiler.core.values._`

`showValues(df).foreach(df => df.show(false))` 

###### Sample Results:

```
+----------+------+---------+----------+                                        
|CustomerID|length|frequency|percentage|
+----------+------+---------+----------+
|17841     |5     |7983     |1.47      |
|14911     |5     |5903     |1.09      |
|14096     |5     |5128     |0.95      |
|12748     |5     |4642     |0.86      |
|14606     |5     |2782     |0.51      |
|15311     |5     |2491     |0.46      |
|14646     |5     |2085     |0.38      |
|13089     |5     |1857     |0.34      |
|13263     |5     |1677     |0.31      |
|14298     |5     |1640     |0.3       |
|15039     |5     |1508     |0.28      |
|14156     |5     |1420     |0.26      |
|18118     |5     |1284     |0.24      |
|14159     |5     |1212     |0.22      |
|14796     |5     |1165     |0.21      |
|15005     |5     |1160     |0.21      |
|16033     |5     |1152     |0.21      |
|14056     |5     |1128     |0.21      |
|14769     |5     |1094     |0.2       |
|17511     |5     |1076     |0.2       |
+----------+------+---------+----------+
only showing top 20 rows

```

```
+---------------+------+---------+----------+                                   
|Country        |length|frequency|percentage|
+---------------+------+---------+----------+
|United Kingdom |14    |495478   |91.43     |
|Germany        |7     |9495     |1.75      |
|France         |6     |8557     |1.58      |
|EIRE           |4     |8196     |1.51      |
|Spain          |5     |2533     |0.47      |
|Netherlands    |11    |2371     |0.44      |
|Belgium        |7     |2069     |0.38      |
|Switzerland    |11    |2002     |0.37      |
|Portugal       |8     |1519     |0.28      |
|Australia      |9     |1259     |0.23      |
|Norway         |6     |1086     |0.2       |
|Italy          |5     |803      |0.15      |
|Channel Islands|15    |758      |0.14      |
|Finland        |7     |695      |0.13      |
|Cyprus         |6     |622      |0.11      |
|Sweden         |6     |462      |0.09      |
|Unspecified    |11    |446      |0.08      |
|Austria        |7     |401      |0.07      |
|Denmark        |7     |389      |0.07      |
|Japan          |5     |358      |0.07      |
+---------------+------+---------+----------+
only showing top 20 rows

```




# SparkExamples

Compares time to compute distance between Austin, TX and 3,173,958 world cities from the Free World Cities Database <https://www.maxmind.com/en/free-world-cities-database>.
City database not included due to large file size. 
Uses great circle method and Vincenty method on WGS84\\
requires PySpark and Geopy packages.
Great circle method is non-iterative and can be vectorized.  
This code compares the time to compute distances of:
1) Great Circle method vectorized 
2) Vincenty Method looped using 1 node
3) Circle Method looped using 1 core
4) Vincenty Method on 4 nodes using Apache Spark
5) Circle Method looped on 4 nodes using Apache Spark

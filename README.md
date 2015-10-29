# SparkExamples

Compares time to compute distances between Austin, TX and 3,173,958 world cities from the Free World Cities Database <https://www.maxmind.com/en/free-world-cities-database>
The city database file is not included in this folder due to large file size (150MB). 

Uses great circle method (6373km radius Earth) and Vincenty method on WGS84

https://en.wikipedia.org/wiki/Great-circle_distance
https://en.wikipedia.org/wiki/Vincenty%27s_formulae

The script requires PySpark and Geopy packages. 
Great circle method is non-iterative and can be vectorized.  
This code compares the time to compute distances of:
1) Great Circle method vectorized 

2) Vincenty Method looped using 1 node

3) Circle Method looped using 1 core

4) Vincenty Method on 4 nodes using Apache Spark

5) Circle Method looped on 4 nodes using Apache Spark

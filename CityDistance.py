# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 15:15:18 2015

@author: kiransathaye
"""
import sys
from random import random
import time
import pyspark as spark
from pyspark import SparkContext
import multiprocessing as MP
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
from mpl_toolkits.basemap import Basemap
import numpy as np
import matplotlib.pyplot as plt
import geopy
from geopy.distance import vincenty
import pandas

def CallDistanceSpark(Frame,CenterLoc):
    try:
        Lat=np.array(Frame['latitude'])
        Long=np.array(Frame['longitude'])
    except:
        Lat=np.array(Frame['Latitude'])
        Long=np.array(Frame['Longitude'])
    NumCores=MP.cpu_count();
    CitiesArray=np.zeros([len(Lat),2])
    CitiesArray[:,0]=Lat;
    CitiesArray[:,1]=Long;

    try:
        sc = SparkContext()
        print('Making sc')
    except:
        print('Spark Context already exists')

    count1 = sc.parallelize(range(len(CitiesArray)),NumCores)
    t=time.time();
    count2=count1.map(lambda x: PointDist(CenterLoc,CitiesArray[x,:]))
    Dist=count2.collect()
    SparkTime=time.time()-t;
    print(['Great Circle Spark Time: ' + str(int(SparkTime*1e3)) +' ms'])

    t=time.time();
    count2=count1.map(lambda x: VincDist(CenterLoc,CitiesArray[x,:]))
    Dist=count2.collect()
    SparkTimeVince=time.time()-t;

    print(['Vincenty Spark Time: ' + str(int(SparkTimeVince*1e3)) +' ms'])
    sc.stop()
    times={'Vincenty Spark Time':SparkTimeVince, 'Circle Spark Time': SparkTime }

    return times

def CallDistances1Core(Frame,CenterLoc):
    try:
        Lat=np.array(Frame['latitude'])
        Long=np.array(Frame['longitude'])
    except:
        Lat=np.array(Frame['Latitude'])
        Long=np.array(Frame['Longitude'])
    Dist1=np.zeros(len(Long))
    Dist2=np.zeros(len(Long))
    Dist3=np.zeros(len(Long))

    t=time.time();

    for i in range(len(Lat)):
        Dist1[i]=PointDist(CenterLoc,[Lat[i],Long[i]])
    CircleLoopTime=time.time()-t;
    print(['Great Circle Loop Time: ' + str(int(CircleLoopTime*1e3)) +' ms'])

    t=time.time();
    for i in range(len(Lat)):
        Dist2[i]=VincDist(CenterLoc,[Lat[i],Long[i]])
    VinceLoopTime=time.time()-t;
    print(['Vincenty Loop Time: ' + str(int(VinceLoopTime*1e3)) +' ms'])

    t=time.time();
    Dist3=CircleVec(CenterLoc,[Lat,Long]);
    VecTime=time.time()-t;
    print(['Great Circle Vector Time: ' + str(int(VecTime*1e3)) +' ms'])

    times={'Cirlce Loop Time': CircleLoopTime, 'Vincenty Loop Time': VinceLoopTime, 'Circle Vector Time': VecTime}

    return times

def CircleVec(Point1,PointsIn):
    lat1=Point1[0]
    lat2=PointsIn[0]
    long1=Point1[1]
    long2=PointsIn[1]

    deg2rad = np.pi/180.0
    phi1 = (90.0 - lat1)*deg2rad
    phi2 = (90.0 - lat2)*deg2rad
    theta1 = long1*deg2rad
    theta2 = long2*deg2rad
    cos = (np.sin(phi1)*np.sin(phi2)*np.cos(theta1 - theta2) +
           np.cos(phi1)*np.cos(phi2))
    arc = np.arccos(cos)*6373;
    return arc # returns great circle distance between points in km

def PointDist(Point1,Point2): # function for great circle distance (Earth Radius 6373km)

    lat1=Point1[0]
    lat2=Point2[0]
    long1=Point1[1]
    long2=Point2[1]
    deg2rad = np.pi/180.0
    phi1 = (90.0 - lat1)*deg2rad
    phi2 = (90.0 - lat2)*deg2rad

    theta1 = long1*deg2rad
    theta2 = long2*deg2rad
    cos = (np.sin(phi1)*np.sin(phi2)*np.cos(theta1 - theta2) +
           np.cos(phi1)*np.cos(phi2))
    arc = np.arccos(cos)*6373;
    return arc # returns great circle distance between points in km

def VincDist(Point1,Point2):
    DistWGS=vincenty(Point1,Point2).km;
    return DistWGS

if __name__ == "__main__":

    try:
        AllTimes=np.zeros(5)
        AllTimes[0:3]=Core1Times.values();
        AllTimes[3:]=TimeSpark.values();
    except:
        AustinTX=np.array([30.25,-97.75]);
        CityData2=pandas.read_csv('worldcitiespop.csv')
        Core1Times=CallDistances1Core(CityData2,AustinTX);
        TimeSpark=CallDistanceSpark(CityData2,AustinTX);
        AllTimes=np.zeros(5)
        AllTimes[0:3]=Core1Times.values();
        AllTimes[3:]=TimeSpark.values();

    plt.close('all')
    plt.bar(np.arange(3),AllTimes[[0,2,1]])
    plt.bar(np.arange(2)+3,AllTimes[3:],color='red')
    plt.text(.3,90,'Circle Vectorized',rotation=90,fontsize=16,backgroundcolor='w')
    plt.text(1.3,85,'Vincenty Loop',rotation=90,fontsize=16,color='w',backgroundcolor='b')
    plt.text(2.3,90,'Circle Loop',rotation=90,fontsize=16,color='k',backgroundcolor='w')
    plt.text(3.3,90,'Vincenty: 4 Nodes',rotation=90,fontsize=16,color='k',backgroundcolor='w')
    plt.text(4.3,90,'Circle: 4 Nodes',rotation=90,fontsize=16,color='k',backgroundcolor='w')
    plt.xticks([],[])
    plt.yticks(np.linspace(0,100,11))
    plt.ylabel('Compute Time (sec)')
    plt.grid()
    plt.savefig('SparkComputeDistance.pdf')


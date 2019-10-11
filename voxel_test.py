from pyspark import SparkContext, SparkConf

# spatial segmentation method1: to map each point to the voxel it belongs to. The dimention of vexol is dm.
#v represents the coordinate of the upperleft grid point which point(x,y,z)belongd to
#d is the dimension of voxel
d=1
def voxel(point):
    pointList=point.split(" ")
    x,y,z=float(pointList[0]),float(pointList[1]),float(pointList[2])
    c=[x,y,z]
    x1,y1,z1=int(x//d),int(y//d),int(z//d)
    v=(x1,y1,z1)
    return (v,c)

#change detection: find whether a point has neighboring points within distance 1m
def changeDetect(voxel):
    # voxels appear in data 2015 but not in data 2007
    if len(voxel[1][0])==0:
        for point2 in voxel[1][1]:
            point2.append(1)
    # voxels appear in data 2007 but not in data 2015
    elif len(voxel[1][1])==0:
        for point1 in voxel[1][0]:
            point1.append(1)
    # voxels appear both in data 2007 and in data 2015
    else:
        # RDD1:data 2007 as compared and data 2015 as referenced
        for point1 in voxel[1][0]:
            x1,y1,z1=point1[0],point1[1],point1[2]
            for point2 in voxel[1][1]:
                x2,y2,z2=point2[0],point2[1],point2[2]
                dist=(x2-x1)*(x2-x1)+(y2-y1)*(y2-y1)+(z2-z1)*(z2-z1)
                if dist<1:
                    point1.append(0)
                    break
            if len(point1)<4:
                point1.append(1)
        #RDD2: data 2015 as compared and data 2007 as referenced
        for point2 in voxel[1][1]:
            x2,y2,z2=point2[0],point2[1],point2[2]
            for point1 in voxel[1][0]:
                x1,y1,z1=point1[0],point1[1],point1[2]
                dist=(x2-x1)*(x2-x1)+(y2-y1)*(y2-y1)+(z2-z1)*(z2-z1)
                if dist<1:
                    point2.append(0)
                    break
            if len(point2)<4:
                point2.append(1)
    return voxel[1]
    
#convert list to string before saving as text file
def convertListToTxt(list):
    txt=""
    if len(list):
        for point in list:
            pointLine='  '.join(str(e) for e in point)+'\n'
            txt+=pointLine
    return txt
#add .txt to the end of the file
# def renameFile(filepath):
#     for filename in os.listdir(filepath):
#         src=filepath + filename
#         dst=src + ".txt"
#         os.rename(src,dst)

if __name__ == "__main__":

  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("voxel_100_f")
  sc = SparkContext(conf=conf)

  #inputFile="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data/316000_234000.txt"
  #inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data/T_316000_233500.txt"
  #inputFile="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/dataMedium/2007"
  #inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/dataMedium/2015"
  inputFile="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data_full/data07"
  inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data_full/data15"
  outputFile1="Voxel_100_f/2007"
  outputFile2="Voxel_100_f/2015"
  points1 = sc.textFile(inputFile)
  points2 = sc.textFile(inputFile2)

  # RDD1&RDD2: map each point to the voxel that contains it
  RDD1=points1.map(voxel)
  RDD2=points2.map(voxel)

  #newRDD: change detect
  newRDD=RDD1.cogroup(RDD2).map(changeDetect)

  result1=newRDD.flatMap(lambda x: list(x[0])).map(lambda point: '  '.join(str(e) for e in point))
  result2=newRDD.flatMap(lambda x: list(x[1])).map(lambda point: '  '.join(str(e) for e in point))

  result1.saveAsTextFile(outputFile1)
  result2.saveAsTextFile(outputFile2)
  # #rename created file
  # renameFile(outputFile1+"/")
  # renameFile(outputFile2+"/")

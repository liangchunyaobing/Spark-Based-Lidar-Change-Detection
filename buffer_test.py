from pyspark import SparkContext, SparkConf


# spatial segmentation method1: to map each point to the voxel it belongs to. The dimention of vexol is dm.
#v represents the coordinate of the upperleft grid point which point(x,y,z)belongd to 
#d represents the edge length of each voxel and r represents the search radius of each point. d>r
d=5
def voxel(point):
    pointList=point.split(" ")
    x,y,z=float(pointList[0]),float(pointList[1]),float(pointList[2])
    c=[x,y,z]
    x1,y1,z1=int(x//d),int(y//d),int(z//d)
    v=(x1,y1,z1)
    return (v,c)

#spatial segmentation 2: to calculate the distance of each point to the voxel it belongs to and determine whether it should be added to nearby voxels
def voxel_buffer(point):
    pointList=point.split(" ")
    x,y,z=float(pointList[0]),float(pointList[1]),float(pointList[2])
    c=[x,y,z]
    x1,y1,z1=int(x//d),int(y//d),int(z//d)
    v=(x1,y1,z1)
    #voxelList is a list of voxel coordinate each point maps to
    voxelList=[(v,c)]
    #d1,d2,d3 is the distance of each point to upperleft point of the voxel; d1_2,d2_2,d3_2 is the diatance of each point to bottomright voxel
    d1,d2,d3=x%d,y%d,z%d
    d1_2,d2_2,d3_2=d-d1,d-d2,d-d3
    if d1<1:
        voxelList.append(((x1-1,y1,z1),c))
        if d2<1:
            d4=d1*d1+d2*d2
            if d4<1:
                voxelList.append(((x1-1,y1-1,z1),c))
                if d3<1:
                    if d4+d3*d3<1: voxelList.append(((x1-1,y1-1,z1-1),c))
                elif d3_2<1:
                    if d4+d3_2*d3_2<1: voxelList.append(((x1-1,y1-1,z1+1),c))
        elif d2_2<1:
            d4=d1*d1+d2_2*d2_2
            if d4<1:
                voxelList.append(((x1-1,y1+1,z1),c))
                if d3<1: 
                    if d4+d3*d3<1: voxelList.append(((x1-1,y1+1,z1-1),c))
                elif d3_2<1:
                    if d4+d3_2*d3_2<1: voxelList.append(((x1-1,y1+1,z1+1),c))
        if d3<1:
            d5=d1*d1+d3*d3
            if d5<1: voxelList.append(((x1-1,y1,z1-1),c))
        elif d3_2<1:
            d5=d1*d1+d3_2*d3_2
            if d5<1: voxelList.append(((x1-1,y1,z1+1),c))

    elif d1_2<1:
        voxelList.append(((x1+1,y1,z1),c))
        if d2<1:
            d4=d1_2*d1_2+d2*d2
            if d4<1:
                voxelList.append(((x1+1,y1-1,z1),c))
                if d3<1:
                    if d4+d3*d3<1: voxelList.append(((x1+1,y1-1,z1-1),c))
                elif d3_2<1:
                    if d4+d3_2*d3_2<1: voxelList.append(((x1+1,y1-1,z1+1),c))
        elif d2_2<1:
            d4=d1_2*d1_2+d2_2*d2_2
            if d4<1:
                voxelList.append(((x1+1,y1+1,z1,x,y,z),c))
                if d3<1: 
                    if d4+d3*d3<1: voxelList.append(((x1+1,y1+1,z1-1),c))
                elif d3_2<1:
                    if d4+d3_2*d3_2<1: voxelList.append(((x1+1,y1+1,z1+1),c))
        if d3<1:
            d5=d1_2*d1_2+d3*d3
            if d5<1: voxelList.append(((x1+1,y1,z1-1),c))
        elif d3_2<1:
            d5=d1_2*d1_2+d3_2*d3_2
            if d5<1: voxelList.append(((x1+1,y1,z1+1),c))

    if d2<1:
        voxelList.append(((x1,y1-1,z1),c))
        if d3<1:
            d6=d2*d2+d3*d3
            if d6<1: voxelList.append(((x1,y1-1,z1-1),c))
        elif d3_2<1:
            d6=d2*d2+d3_2*d3_2
            if d6<1: voxelList.append(((x1,y1-1,z1+1),c))
    elif d2_2<1:
        voxelList.append(((x1,y1+1,z1),c))
        if d3<1:
            d6=d2_2*d2_2+d3*d3
            if d6<1: voxelList.append(((x1,y1+1,z1-1),c))
        elif d3_2<1:
            d6=d2_2*d2_2+d3_2*d3_2
            if d6<1: voxelList.append(((x1,y1+1,z1+1),c))

    if d3<1: voxelList.append(((x1,y1,z1-1),c))
    elif d3_2<1: voxelList.append(((x1,y1,z1+1),c))
    return voxelList

#after flatmap, convert string to tuple
def convertToTuple(pairString):
    pairStringList=pairString.split(",")
    return((int(pairStringList[0]),int(pairStringList[1]),int(pairStringList[2])),[float(pairStringList[3]),float(pairStringList[4]),float(pairStringList[5])])

#change detection: find whether a point has neighboring points within distance 1m
def changeDetect(voxel):
    if len(voxel[0])!=0:
        # voxels appear in data 1 but not in data 2
        if len(voxel[1])==0:
            for point1 in voxel[0]:
                point1.append(1)
        # voxels appear both in data 2007 and in data 2015
        else:
            # data 1 as compared and data 2 as referenced
            for point1 in voxel[0]:
                x1,y1,z1=point1[0],point1[1],point1[2]
                for point2 in voxel[1]:
                    x2,y2,z2=point2[0],point2[1],point2[2]
                    if abs(x2-x1)<1 and abs(y2-y1)<1 and abs(z2-z1)<1:
                        dist=(x2-x1)*(x2-x1)+(y2-y1)*(y2-y1)+(z2-z1)*(z2-z1)
                        if dist<1:
                            point1.append(0)
                            break
                if len(point1)<4:
                    point1.append(1)
    return voxel
    
#convert list to string before saving as text file
def convertListToTxt(list):
    if len(list):
        for point in list:
            pointLine='  '.join(str(e) for e in point)+'\n'
            txt+=pointLine
    return txt

def renameFile(filepath):
    for filename in os.listdir(filepath):
        src=filepath + filename
        dst=src + ".txt"
        os.rename(src,dst)

if __name__ == "__main__":

  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("buffer_100_f")
  sc = SparkContext(conf=conf)
  
  #inputFile1="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/2007.asc"
  #inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/2015.asc"
  #inputFile1="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data/316000_234000.txt"
  #inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data/T_316000_233500.txt"
 # inputFile1="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/dataMedium/2007"
  #inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/dataMedium/2015"
  inputFile1="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data_full/data07"
  inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data_full/data15"
  outputFile1="buffer_100_f/2007"
  outputFile2="buffer_100_f/2015"
  points1 = sc.textFile(inputFile1)
  points2 = sc.textFile(inputFile2)

  #method 1: first map RDD using voxel_buffer and then generate voxelRDD and BufferRDD from RDD 
  # RDD1&RDD2: map each point to the voxel that contains it
  RDD1=points1.map(voxel_buffer)
  RDD2=points2.map(voxel_buffer)
  # VoxelRDD1&VoxelRDD2: map each point to the voxel that contains it
  VoxelRDD1=RDD1.map(lambda x: x[0])
  VoxelRDD2=RDD2.map(lambda x: x[0])
  #BufferRDD1&BufferRDD2: map each point to the voxel that is within 1m
  BufferRDD1=RDD1.flatMap(lambda x: x)
  BufferRDD2=RDD2.flatMap(lambda x: x)

  #method 2: map VoxelRDD using voxel and map BufferRDD from voxel_buffer
  # VoxelRDD1=RDD1.map(voxel)
  # VoxelRDD2=RDD2.map(voxel)
  # BufferRDD1=RDD1.flatMap(voxel_buffer)
  # BUfferRDD2=RDD2.flatMap(voxel_buffer)

  #ChangeRDD1&ChangeRDD2: change detection
  ChangeRDD1=VoxelRDD1.cogroup(BufferRDD2).mapValues(changeDetect)
  ChangeRDD2=VoxelRDD2.cogroup(BufferRDD1).mapValues(changeDetect)
  #result1&result2: constract point information
  result1=ChangeRDD1.flatMap(lambda x: list(x[1][0])).map(lambda point: '  '.join(str(e) for e in point))
  result2=ChangeRDD2.flatMap(lambda x: list(x[1][0])).map(lambda point: '  '.join(str(e) for e in point))
  # result1=ChangeRDD1.map(lambda x: list(x[1][0])).map(convertListToTxt)
  # result2=ChangeRDD2.map(lambda x: list(x[1][0])).map(convertListToTxt)
  #save as text file
  result1.saveAsTextFile(outputFile1)
  result2.saveAsTextFile(outputFile2)




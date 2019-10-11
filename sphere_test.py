# This code is for change detection on big data system which tries to overcome the problem of space discontinuity
from pyspark import SparkContext, SparkConf

# spatial segmentation method1: to map each point to the voxel it belongs to. The dimention of vexol is dm.
#v represents the coordinate of the upperleft grid point which point(x,y,z)belongd to 
#d represents the edge length of each voxel and r represents the search radius of each point. d>r
d,dMin=2,1
r=pow((0.866*dMin),2)
#divide points into voxels with dMin dimension
def voxel(point):
    voxelList=[]
    pointList=point.split(" ")
    x,y,z=float(pointList[0]),float(pointList[1]),float(pointList[2])
    c=(x,y,z)
    x1,y1,z1=int(x//dMin),int(y//dMin),int(z//dMin)
    x0,y0,z0=pow((x-x1*dMin),2),pow((y-y1*dMin),2),pow((z-z*dMin),2)
    x2,y2,z2=pow((x0-dMin),2),pow((y0-dMin),2),pow((z0-dMin),2)
    if x0+y0+z0<r:
        voxelList.append(((x1,y1,z1),c))
    elif x2+y2+z2<r:
        voxelList.append(((x1+dMin,y1+dMin,z1+dMin),c))
    if x2+y0+z0<r:
        voxelList.append(((x1+dMin,y1,z1),c))
    elif x0+y2+z2<r:
        voxelList.append((x1,y1+dMin,z1+dMin))
    if x0+y2+z0<r:
        voxelList.append(((x1,y1+dMin,z1),c))
    elif x2+y0+z2<r:
        voxelList.append(((x1+dMin,y1,z1+dMin),c))
    if x0+y0+z2<r:
        voxelList.append(((x1,y1,z1+dMin),c))
    elif x2+y2+z0<r:
        voxelList.append(((x1+dMin,y1+dMin,z1),c))
    return voxelList

def changeDetect(voxel):
    v1,v2=[],[]
    # voxels appear in data 2015 but not in data 2007
    if len(voxel[1][0])==0:
        for point2 in voxel[1][1]:
            v2.append((point2,1))
    # voxels appear in data 2007 but not in data 2015
    elif len(voxel[1][1])==0:
        for point1 in voxel[1][0]:
            v1.append((point1,1))
    # voxels appear both in data 2007 and in data 2015
    else:
        # RDD1:data 2007 as compared and data 2015 as referenced
        for point1 in voxel[1][0]:
            x1,y1,z1=point1[0],point1[1],point1[2]
            for point2 in voxel[1][1]:
                x2,y2,z2=point2[0],point2[1],point2[2]
                dist=(x2-x1)*(x2-x1)+(y2-y1)*(y2-y1)+(z2-z1)*(z2-z1)
                if dist<1:
                    v1.append((point1,0))
                    break
            if len(point1)>2:
                v1.append((point1,1))
        #RDD2: data 2015 as compared and data 2007 as referenced
        for point2 in voxel[1][1]:
            x2,y2,z2=point2[0],point2[1],point2[2]
            for point1 in voxel[1][0]:
                x1,y1,z1=point1[0],point1[1],point1[2]
                dist=(x2-x1)*(x2-x1)+(y2-y1)*(y2-y1)+(z2-z1)*(z2-z1)
                if dist<1:
                    v2.append((point2,0))
                    break
            if len(point2)>2:
                v2.append((point1,1))
    return (v1,v2)

def changeDetectFinal(point):
    changeValue=1
    for value in point[1]:
        if value==0:
            changeValue=0
            break
    p=list(point[0])+[changeValue]
    return p

#add .txt to the end of the file
def renameFile(filepath):
    for filename in os.listdir(filepath):
        src=filepath + filename
        dst=src + ".txt"
        os.rename(src,dst)
# STEP 1 map each point to the voxel with a dimension of 1m that contains it
# STEP 2 group voxels into subdomains with a dimension of 5*5*5
# STEP 3 create a one-voxel buffer for each subdomain
if __name__ == "__main__":

  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("sphere_60_f")
  sc = SparkContext(conf=conf)

  #inputFile1="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/2007.asc"
  #inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/2015.asc"
  #inputFile1="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data/316000_234000.txt"
  #inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data/T_316000_233500.txt"
  #inputFile1="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/dataMedium/2007"
  #inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/dataMedium/2015"
  inputFile1="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data_full/data07"
  inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data_full/data15"
  outputFile1="sphere_60_f/2007"
  outputFile2="sphere_60_f/2015"
  points1 = sc.textFile(inputFile1)
  points2 = sc.textFile(inputFile2)

  RDD1=points1.flatMap(voxel)
  RDD2=points2.flatMap(voxel)
  cogroupRDD=RDD1.cogroup(RDD2).map(changeDetect)
  changeRDD1=cogroupRDD.flatMap(lambda x : x[0]).groupByKey().map(changeDetectFinal).map(lambda point: '  '.join(str(e) for e in point))
  changeRDD2=cogroupRDD.flatMap(lambda x : x[1]).groupByKey().map(changeDetectFinal).map(lambda point: '  '.join(str(e) for e in point))
  # save as text file
  changeRDD1.saveAsTextFile(outputFile1)
  changeRDD2.saveAsTextFile(outputFile2)


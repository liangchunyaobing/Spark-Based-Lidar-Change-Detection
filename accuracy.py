# This code is for change detection on big data system which tries to overcome the problem of space discontinuity
from pyspark import SparkContext, SparkConf

# spatial segmentation method1: to map each point to the voxel it belongs to. The dimention of vexol is dm.
#v represents the coordinate of the upperleft grid point which point(x,y,z)belongd to 
#d represents the edge length of each voxel and r represents the search radius of each point. d>r
def changepair(point):
    pointList=point.split("  ")
    x,y,z,n=round(float(pointList[0]),3),round(float(pointList[1]),3),round(float(pointList[2]),3),int(float(pointList[3]))
    point=((x,y,z),n)
    return point

def cloudpair(point):
    pointList=point.split(" ")
    x,y,z,n=round(float(pointList[0]),3),round(float(pointList[1]),3),round(float(pointList[2]),3),0 if float(pointList[3])<0.999 else 1
    point=((x,y,z),n)
    return point

def accuracy(point):
    pointPair=point[1]
    if len(pointPair[0]) and len(pointPair[1]):
        if list(pointPair[0])[0]==1:
            #true positive
            if list(pointPair[1])[0]==1:
                point=(0,1)
            #false positive
            else:
                point=(1,1)
        else:
            #false negative
            if list(pointPair[1])[0]==1:
                point=(2,1)
            #true negative
            else:
                point=(3,1)
    else:
        point=(4,1)
    return point

# STEP 1 map each point to the voxel with a dimension of 1m that contains it
# STEP 2 group voxels into subdomains with a dimension of 5*5*5
# STEP 3 create a one-voxel buffer for each subdomain
if __name__ == "__main__":

  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("accuracy_sphere")
  sc = SparkContext(conf=conf)

  inputFile1="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/sphere_100_f/2007"
  inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data_cloud/data07"
  inputFile3="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/sphere_100_f/2015"
  inputFile4="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data_cloud/data15"
  outputFile1="accuracy_sphere/07"
  outputFile2="accuracy_sphere/15"
  points1 = sc.textFile(inputFile1)
  points2 = sc.textFile(inputFile2)
  points3 = sc.textFile(inputFile3)
  points4 = sc.textFile(inputFile4)

  RDD1=points1.map(changepair)
  RDD2=points2.map(cloudpair)
  RDD3=points3.map(changepair)
  RDD4=points4.map(cloudpair)

  cogroupRDD1=RDD1.cogroup(RDD2).map(accuracy).reduceByKey(lambda x,y: x+y)
  cogroupRDD2=RDD3.cogroup(RDD4).map(accuracy).reduceByKey(lambda x,y: x+y)
  # save as text file
  cogroupRDD1.saveAsTextFile(outputFile1)
  cogroupRDD2.saveAsTextFile(outputFile2)


from pyspark import SparkContext, SparkConf

# spatial segmentation method1: to map each point to the voxel it belongs to. The dimention of vexol is dm.
#v represents the coordinate of the upperleft grid point which point(x,y,z)belongd to 
#d represents the edge length of each voxel and r represents the search radius of each point. d>r
d=5
dMin=1
#divide points into voxels with dMin dimension
def voxel(point):
    pointList=point.split(" ")
    x,y,z=float(pointList[0]),float(pointList[1]),float(pointList[2])
    c=[x,y,z]
    x1,y1,z1=int(x//dMin),int(y//dMin),int(z//dMin)
    v=(x1,y1,z1)
    return (v,c)
#group tiles into dm subdomains
def subdomain(subdomain):
    x,y,z=int(voxel[0][0]//d),int(voxel[0][1]//d),int(voxel[0][2]//d)
    s=(x,y,z)
    return(s,voxel)
#group tiles into dm subdomains with a tile buffer
def subdomainBuffer(voxel):
    x,y,z=int(voxel[0][0]//d),int(voxel[0][1]//d),int(voxel[0][2]//d)
    s=[((x,y,z),voxel)]
    d1,d2,d3=int(voxel[0][0]%d),int(voxel[0][0]%d),int(voxel[0][0]%d)
    if d1==1:
        s.append(((x-1,y,z),voxel))
        if d2==1:
            s.append(((x-1,y-1,z),voxel))
            if d3==1:
                s.append(((x-1,y-1,z-1),voxel))
            elif d3==d-1:
                s.append(((x-1,y-1,z+1),voxel))
        elif d2==d-1:
            s.append(((x-1,y+1,z),voxel))
            if d3==1:
                s.append(((x-1,y+1,z-1),voxel))
            elif d3==d-1:
                s.append(((x-1,y+1,z+1),voxel))
    elif d1==d-1:
        s.append(((x+1,y,z),voxel))
        if d2==1:
            s.append(((x+1,y-1,z),voxel))
            if d3==1:
                s.append(((x+1,y-1,z-1),voxel))
            elif d3==d-1:
                s.append(((x+1,y-1,z+1),voxel))
        elif d2==d-1:
            s.append(((x+1,y+1,z),voxel))
            if d3==1:
                s.append(((x+1,y+1,z-1),voxel))
            elif d3==d-1:
                s.append(((x+1,y+1,z+1),voxel))
    if d2==1:
        s.append(((x,y-1,z),voxel))
        if d3==1:
            s.append(((x,y-1,z-1),voxel))
        elif d3==d-1:
            s.append(((x,y-1,z+1),voxel))
    elif d2==d-1:
        s.append(((x,y+1,z),voxel))
        if d3==1:
            s.append(((x,y+1,z-1),voxel))
        elif d3==d-1:
            s.append(((x,y+1,z+1),voxel))
    if d3==1:
        s.append(((x,y,z-1),voxel))
    elif d3==d-1:
        s.append(((x,y,z+1),voxel))
    return s
#for each voxel in subdomain in data1, first find its neighboring voxels in data2, then compute the disdance of point clouds
def changeDetect(subdomain):
    if len(subdomain[1][0]):
        # voxels appear in data 1 but not in data 2
        if len(subdomain[1][1])==0:
            for voxel1 in subdomain[1][0]:
                for point1 in voxel1[1]:
                     point1.append(1)
        # voxels appear both in data 2007 and in data 2015
        else:
            for voxel1 in subdomain[1][0]:
                xv1,yv1,zv1=voxel1[0][0],voxel1[0][1],voxel1[0][2]
                pointNumber=len(list(voxel1[1]))                
                pointCount=0
                voxelCount=0
                for voxel2 in subdomain[1][1]:
                    xv2,yv2,zv2=voxel2[0][0],voxel2[0][1],voxel2[0][2]
                    if abs(xv2-xv1)<=1 and abs(yv2-yv1)<=1 and abs(zv2-zv1)<=1:
                        voxelCount+=1
                        for point1 in voxel1[1]:
                            if len(point1)<4:
                                x1,y1,z1=point1[0],point1[1],point1[2]
                                for point2 in voxel2[1]:
                                    x2,y2,z2=point2[0],point2[1],point2[2]
                                    dist=(x2-x1)*(x2-x1)+(y2-y1)*(y2-y1)+(z2-z1)*(z2-z1)
                                    if dist<dMin:
                                        point1.append(0)
                                        pointCount+=1                            
                                        break
                    if pointCount==pointNumber or voxelCount==27:
                        break
                if pointCount!=pointNumber:
                    for point1 in voxel1[1]:
                        if len(point1)<4:
                            point1.append(1)    
    return subdomain[1][0]          
# add .txt at the end of output file
def renameFile(filepath):
    for filename in os.listdir(filepath):
        src=filepath + filename
        dst=src + ".txt"
        os.rename(src,dst)

if __name__ == "__main__":

  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("subdomain_40_f")
  sc = SparkContext(conf=conf)

  #inputFile="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/2007.asc"
  #inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/2015.asc"
  #inputFile="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data/316000_234000.txt"
  #inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data/T_316000_233500.txt"
  #inputFile="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/dataMedium/2007"
  #inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/dataMedium/2015"
  inputFile="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data_full/data07"
  inputFile2="hdfs://babar.es.its.nyu.edu:8020/user/yl7280/data_full/data15"
  outputFile1="subdomain_40_f/2007"
  outputFile2="subdomain_40_f/2015"
  points1 = sc.textFile(inputFile)
  points2 = sc.textFile(inputFile2)
  # RDD1&RDD2: map each point to the voxel that contains it and then group voxel into subdomains with buffer
  RDD1=points1.map(voxel).groupByKey().map(subdomainBuffer)
  RDD2=points2.map(voxel).groupByKey().map(subdomainBuffer)
  # SubdomainRDD1: map each voxel to a subdomain that contains it
  SubdomainRDD1=RDD1.map(lambda x: x[0])
  SubdomainRDD2=RDD2.map(lambda x: x[0])
  # #BufferRDD1&BufferRDD2: map each voxel to a subdomain that has a buffer
  BufferRDD1=RDD1.flatMap(lambda x: x)
  BufferRDD2=RDD2.flatMap(lambda x: x)
  #ChangeRDD1&ChangeRDD2: change detection, return a list of(voxel,points)
  ChangeRDD1=SubdomainRDD1.cogroup(BufferRDD2).flatMap(changeDetect)
  ChangeRDD2=SubdomainRDD2.cogroup(BufferRDD1).flatMap(changeDetect)
  # result1&result2: constract point information
  result1=ChangeRDD1.flatMap(lambda x: list(x[1])).map(lambda point: '  '.join(str(e) for e in point))
  result2=ChangeRDD2.flatMap(lambda x: list(x[1])).map(lambda point: '  '.join(str(e) for e in point))
  #save as text file
  result1.saveAsTextFile(outputFile1)
  result2.saveAsTextFile(outputFile2)
  # rename created file
#   renameFile(outputFile1+"/")
#   renameFile(outputFile2+"/")

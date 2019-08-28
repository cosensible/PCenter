# -*- coding : utf-8 -*-

import numpy as np
import random
import json
import time
import os

INFINITY=64000
iterTimes=10000 # 迭代次数

def FloydWarshall(G):
    for k in range(N):
        for i in range(N):
            for j in range(N):
                if G[i][j] > G[i][k] + G[k][j]:
                    G[i][j] = G[i][k] + G[k][j]

def reservoirSamp(sampList):
    if sampList is None:
        return None
    res=sampList[0]
    for i in range(1,len(sampList)):
        j=random.randint(0,i)
        if j<1:
            res=sampList[i]
    return res

def readData(filePath):
    global N,P,G,F,D,M,T,S
    for file in os.listdir(filePath):
        fileName=file.split('.')[0]
        file=os.path.join(filePath,file)
        with open(file,'r') as f:
            data=json.load(f)
        graph=data['graph']
        P=data['centerNum']
        edges = graph['edges']
        N=0
        for e in edges:
            s = e['source']
            t = e['target']
            N = max(s, t, N)
        G=np.full([N,N],INFINITY,dtype=np.int32)
        for i in range(N):
            G[i][i]=0
        for e in edges:
            s=e['source']-1
            t=e['target']-1
            l=e['length']
            G[s][t]=G[t][s]=l
        FloydWarshall(G)
         # F表记录最近和次近服务节点，D表记录对应的距离
        F = np.full([2, N],fill_value=N,dtype=np.int32)
        D = np.full([2, N],fill_value=INFINITY,dtype=np.int32)
        # M[f]表示删除服务节点f后产生的最长服务边
        M=[0]*N
        # 禁忌表
        T=np.zeros([N,N],dtype=np.int32)
        S=[]
        yield fileName
        
def addFacility(f):
    global Sc_cur
    S.append(f)
    for v in range(N):
        if G[f][v]<D[0][v]:
            D[1][v]=D[0][v]
            F[1][v]=F[0][v]
            D[0][v]=G[f][v]
            F[0][v]=f
        elif G[f][v]<D[1][v]:
            D[1][v]=G[f][v]
            F[1][v]=f
    Sc_cur=max(D[0])

def findNext(v,f):
    distToServer=[G[v][s] for s in S]
    ds=zip(distToServer,S)
    ds=sorted(ds)
    for t in ds:
        if(t[1]!=f):
            F[1][v]=t[1]
            D[1][v]=t[0]
            break
        else:
            continue


def removeFacility(f):
    global Sc_cur
    S.remove(f)
    for v in range(N):
        if F[0][v]==f:
            D[0][v]=D[1][v]
            F[0][v]=F[1][v]
            findNext(v,F[0][v])
        elif F[1][v]==f:
            findNext(v,F[0][v])
    Sc_cur=max(D[0])

def candidate():
    maxDist=max(D[0])
    vertexes=[i for i,v in enumerate(D[0]) if v==maxDist]
    vertex=reservoirSamp(vertexes)
    return [i for i,d in enumerate(G[vertex]) if d<maxDist]

def initial():
    global Sc_best
    server=random.randint(0,N-1)
    addFacility(server)
    for _ in range(P-1):
        Nwk=candidate()
        server1=reservoirSamp(Nwk)
        addFacility(server1)
    Sc_best=Sc_cur

	
def findPair(candidate,step):
    global tabuList,notTabuList
    tabuList=notTabuList=[]
    C=INFINITY
    for i in candidate:
        addFacility(i)
        S1=S.copy()
        S1.remove(i)
        for f in S1:
            indexes=[i for i,v in enumerate(F[0]) if v==f]
            # M[f]删除服务节点f后产生的最长服务边
            dists=[v for i,v in enumerate(D[1]) if i in indexes]
            M[f]=max(dists)
            newSc=max(Sc_cur,M[f]) # 交换f,i后产生的目标函数
            
            if newSc<C:
                C=newSc
                if T[f][i]>step:
                    tabuList=[(f,i,newSc)]
                else:
                    notTabuList=[(f,i,newSc)]
            elif newSc==C:
                if T[f][i]>step:
                    tabuList.append((f,i,newSc))
                else:
                    notTabuList.append((f,i,newSc))
        removeFacility(i)


if __name__=="__main__":
    path=r'D:\MyFiles\PythonProjects\PCenter''\\'
    smallIns = 'smallInstance'
    halfIns='halfInstance'
    allIns='allInstance'
    testIns='testInstance'
    # 前40个实例的最优值
    opt = [127,98,93,74,48,84,64,55,37,20,59,51,36,26,18,\
            47,39,28,18,13,40,38,22,15,11,38,32,18,13,9,\
            30,29,15,11,30,27,15,29,23,13]
    for ins in readData(path+smallIns):
        optIndex=int(ins[-2:])-1
        startTime=time.clock()
        initial()
        iter=0
        for i in range(iterTimes):
            iter=i
            tt=P//5+5+random.randint(1,P//10+5)
            findPair(candidate(),i)
            m=()
            if notTabuList and notTabuList[0][2]<Sc_best:
                m=reservoirSamp(notTabuList)
            elif tabuList and tabuList[0][2]<Sc_best:
                m=reservoirSamp(tabuList)
            elif notTabuList:
                m=reservoirSamp(notTabuList)
            if m:	
                addFacility(m[1])
                removeFacility(m[0])
                T[m[0]][m[1]] = T[m[1]][m[0]] = tt+i
                if(Sc_cur<Sc_best):
                    Sc_best=Sc_cur
            if Sc_best<=opt[optIndex]:
                break
        endTime=time.clock()

        with open(".\\solution\\{}1.txt".format(smallIns), "a", encoding="UTF-8") as f:
            f.write('Run at:'+str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            f.write('\nIns:'+str(ins)+'\t')
            f.write('N = '+str(N)+'\t')
            f.write('P = ' + str(P)+'\n')
            f.write('Sc = ' + str(Sc_best) + '\n')
            f.write('S = '+str(S)+'\n')
            f.write('iterate times = ' + str(iter) + '\n')
            f.write('CPU time = '+str(endTime-startTime)+'\n\n')
        f.close()  # 关闭文件夹

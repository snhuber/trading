N = 15
n = 5
nColss = []
for i in range(N):
    for j in range(N):
        for k in range(N):
            for l in range(N):
                for m in range(N):
                    nCols = len(set([i,j,k,l,m]))
                    nColss.append(nCols)
                    pass
                pass
            pass
        pass
    pass

NTotal = len(nColss)

import numpy as np
E = 0
for nCols in np.arange(1,6):
    print (nCols)
    NNCols = nColss.count(nCols)
    print (nCols, NNCols)
    E += nCols * NNCols/NTotal
    pass

print (E)

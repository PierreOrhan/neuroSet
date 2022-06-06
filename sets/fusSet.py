import numpy as np

def dic_iterate(dic,ks):
    if len(ks)<=1:
        if ks[0] == None:
            return dic
        elif type(ks[0])==list:
            return {k:dic[k] for k in ks[0]}
        return dic[ks[0]]
    if ks[0]==None:
        return {k:dic_iterate(dic[k],ks[1:]) for k in dic.keys()}
    elif type(ks[0]) == list:
        return {k: dic_iterate(dic[k],ks[1:]) for k in ks[0]}
    return dic_iterate(dic[ks[0]],ks[1:])

class fusSet():
    def __init__(self):
        self.dic = {}
    #def load(self):
    #def load_empty(self,dir):
    #def load_from(self,f):
        # f needs to provide the exact {animal,sess,slice} format
    #def create_empty(self):

    #def yield(self):

    #For v2 #def par_yield(self):
    #parallel yielding

    def __getitem__(self,k):
        ## obtain a subset of the tree from a list
        # use the key None to ask for all elements in the list
        if type(k)==list:
            return dic_iterate(self.dic,k)
        else:
            if k==None:
                return self.dic
            return self.dic[k]

    

# def to(inSet::fusSet,f):
# def to(inSet::fusSet,f,outSet::fusSet):


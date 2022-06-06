import numpy as np
import os
import scipy.io
import mat73

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

def dic_set(dic,ks,value):
    if len(ks)<=1:
        if ks[0] == None:
            raise Exception("last key need to be not_empty")
        elif type(ks[0])==list:
            raise Exception("last key need to be single")
        dic[ks[0]] = value
        return None
    if ks[0]==None:
        for k in dic.keys():
            return dic_set(dic[k], ks[1:], value)
    elif type(ks[0]) == list:
        for k in ks[0]:
            return dic_set(dic[k],ks[1:],value)
    return dic_set(dic[ks[0]], ks[1:], value)

class fusSet():
    ## the idea of a fusSet is to keep a structure of 3 dictionary inside each other
    # But the  final leaf element should be dictionary which share the same keys

    def __init__(self):
        self.dic = {}

    def load(self,dir,v73=True):
        # the dir should contain mat file with the following structure:
        # animal_session_slice.mat
        ls = os.listdir(dir)
        animals = np.unique([l.split("_")[0] for l in ls])

        dic = {a:None for a in animals}
        for a in animals:
            all_animal = np.array([l.split("_")[0] for l in ls])
            ls_a = np.where(np.equal(all_animal,a))[0]
            sessions = np.unique([l.split("_")[1] for l in ls[ls_a]])
            dic[a] = {s:None for s in sessions}
            for sess in sessions:
                all_sess = np.array([l.split("_")[1] for l in ls[ls_a]])
                ls_s = np.where(np.equal(all_sess, sess))[0]
                slices = np.unique([l.split("_")[2] for l in ls[ls_a][ls_s]])
                dic[a][sess] = {sl.split(".mat")[0]:None for sl in slices}
                for sl in slices:
                    if not v73:
                        dic[a][sess][sl] = scipy.io.loadmat(os.path.join(dir,a+"_"+sess+"_"+sl+".mat"))
                    else:
                        dic[a][sess][sl] = mat73.loadmat(os.path.join(dir, a + "_" + sess + "_" + sl + ".mat"))
        return dic

    def load_empty(self,dir,v73=True):
        ls = os.listdir(dir)
        animals = np.unique([l.split("_")[0] for l in ls])

        dic = {a: None for a in animals}
        for a in animals:
            all_animal = np.array([l.split("_")[0] for l in ls])
            ls_a = np.where(np.equal(all_animal, a))[0]
            sessions = np.unique([l.split("_")[1] for l in ls[ls_a]])
            dic[a] = {s: None for s in sessions}
            for sess in sessions:
                all_sess = np.array([l.split("_")[1] for l in ls[ls_a]])
                ls_s = np.where(np.equal(all_sess, sess))[0]
                slices = np.unique([l.split("_")[2] for l in ls[ls_a][ls_s]])
                dic[a][sess] = {sl.split(".mat")[0]: None for sl in slices}
                for sl in slices:
                    if not v73:
                        mat = scipy.io.loadmat(os.path.join(dir, a + "_" + sess + "_" + sl + ".mat"))
                        dic[a][sess][sl] = {k:None for k in mat.keys()}
                        del mat
                    else:
                        mat = scipy.io.loadmat(os.path.join(dir, a + "_" + sess + "_" + sl + ".mat"))
                        dic[a][sess][sl] = {k:None for k in mat.keys()}
                        del mat
        return dic

    @classmethod
    def from_dic(self,dic):
        fS = fusSet()
        fS.dic = dic
        return fS

    def __getitem__(self,k):
        ## Obtain a subset of the tree dictionary from a list
        # use the key None to ask for all elements in the list
        if type(k)==list or type(k)==tuple:
            return dic_iterate(self.dic,k)
        else:
            if k==None:
                return self.dic
            return self.dic[k]

    def __setitem__(self, key, value):
        if type(key)==list or type(key)==tuple:
            dic_set(self.dic,list(key),value)
        else:
            raise Exception(" not well written")

    def keys(self):
        return self.dic.keys()

    def final_keys(self):
        leaf = next(iter(self))
        return leaf.keys()

    def __iter__(self):
        ## iterating the object as in for ? in x
        for a in self.dic.keys():
            for sess in self.dic[a].keys():
                for slice in self.dic[a][sess].keys():
                    yield self.dic[a][sess][slice]

    def c_yield(self,ks):
        ## conditional yielding
        for a in self.dic.keys():
            if ks[0]==None or a in ks[0]:
                for sess in self.dic[a].keys():
                    if ks[1] == None or a in ks[1]:
                        for slice in self.dic[a][sess].keys():
                            if ks[2] == None or a in ks[2]:
                                yield self.dic[a][sess][slice]

    def merge_animal(self,k,n:str,fmerge,axis):
        ## assumes a similar substructure across animals:
        animals = list(self.dic.keys())
        sessions = self.dic[animals[0]].keys()
        slices = self[animals[0],sessions[0]].keys()
        nDic = {n: {sess: {slice: {k: None} for slice in slices} for sess in sessions}}
        fset = fusSet.from_dic(nDic)
        for sess in sessions:
            for slice in slices:
                to_stack = []
                for a in animals:
                    to_stack += [self.dic[a][sess][slice][k]]
                fset[n, sess, slice, k] = fmerge(to_stack, axis=axis)
        return fset
    def merge_session(self,k,n:str,fmerge,axis):
        animals = list(self.dic.keys())
        nDic = {a:None for a in animals}
        for a in animals:
            sessions = self.dic[a].keys()
            ## assumes a similar session across animals:
            slices = self[a,sessions[0]].keys()
            nDic[a] = {n:{slice:{k:None} for slice in slices}}
            for slice in slices:
                to_stack = []
                for sess in sessions:
                    to_stack += [self.dic[a][sess][slice][k]]
                nDic[a][n][slice][k] = fmerge(to_stack, axis=axis)
        fset = fusSet.from_dic(nDic)
        return fset
    def merge_slice(self,k,n:str,fmerge,axis):
        animals = list(self.dic.keys())
        nDic = {a:None for a in animals}
        for a in animals:
            sessions = self.dic[a].keys()
            nDic[a] = {sess:{n:{k:None}} for sess in sessions}
            for sess in sessions:
                to_stack = []
                for slice in self.dic[a][sess].keys():
                    to_stack += [self.dic[a][sess][slice][k]]
                nDic[a][sess][n][k] = fmerge(to_stack, axis=axis)
        fset = fusSet.from_dic(nDic)
        return fset

    #For v2 #def par_yield(self):
    #parallel yielding


def to(inSet:fusSet,f,k):
    for b in inSet:
        b[k] = f(b[k])
    return inSet
def to_(inSet:fusSet,outSet:fusSet,f,k):
    for bin,bout in zip(inSet,outSet):
        bout[k] = f(bin[k])
    return outSet


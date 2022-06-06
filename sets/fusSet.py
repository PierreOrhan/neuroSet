import numpy as np
import os
import scipy.io
import mat73

def dic_get(dic,ks):
    if len(ks) <= 1:
        return dic[ks[0]]
    return dic_get(dic[ks[0]],ks[1:])

def dic_iterate(dic,ks):
    if len(ks)<=1:
        if ks[0] == None:
            return dic
        elif type(ks[0])==list:
            return {k:dic[k] for k in ks[0]}
        return {ks[0]: dic[ks[0]]}
    if ks[0]==None:
        return {k:dic_iterate(dic[k],ks[1:]) for k in dic.keys()}
    elif type(ks[0]) == list:
        return {k: dic_iterate(dic[k],ks[1:]) for k in ks[0]}
    return dic_iterate({ks[0]: dic[ks[0]]},ks[1:])

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

    def load(self,dir,v73=True,fillNone=False):
        # the dir should contain mat file with the following structure:
        # animal_session_slice.mat
        ls = np.array(os.listdir(dir))
        animals = np.unique([l.split("_")[0] for l in ls])
        animals = animals[np.where([a!="param" for a in animals])[0]]
        dic = {a:None for a in animals}
        for a in animals:
            all_animal = np.array([l.split("_")[0] for l in ls])
            ls_a = np.where([aa==a for aa in all_animal])[0]
            sessions = np.unique([l.split("_")[1] for l in ls[ls_a]])
            dic[a] = {s:None for s in sessions}
            for sess in sessions:
                all_sess = np.array([l.split("_")[1] for l in ls[ls_a]])
                ls_s = np.where([asess==sess for asess in all_sess])[0]
                slices = np.unique([l.split("_")[2] for l in ls[ls_a][ls_s]])
                dic[a][sess] = {sl.split(".mat")[0]:None for sl in slices}
                for sl in dic[a][sess].keys():
                    if fillNone:
                        if not v73:
                            mat = scipy.io.loadmat(os.path.join(dir, a + "_" + sess + "_" + sl + ".mat"))
                            dic[a][sess][sl] = {k: None for k in mat.keys()}
                            del mat
                        else:
                            mat = scipy.io.loadmat(os.path.join(dir, a + "_" + sess + "_" + sl + ".mat"))
                            dic[a][sess][sl] = {k: None for k in mat.keys()}
                            del mat
                    else:
                        if not v73:
                            dic[a][sess][sl] = scipy.io.loadmat(os.path.join(dir,a+"_"+sess+"_"+sl+".mat"))
                        else:
                            dic[a][sess][sl] = mat73.loadmat(os.path.join(dir, a + "_" + sess + "_" + sl + ".mat"))
        self.dic = dic
        return None

    def load_frompathDic(self,path_dic,v73=True,fillNone=False):
        dic = {a:None for a in path_dic.keys()}
        for a in path_dic.keys():
            dic[a] = {s:None for s in path_dic[a].keys()}
            for sess in path_dic[a].keys():
                dic[a][sess] = {sl.split(".mat")[0]:None for sl in path_dic[a][sess].keys()}
                for sl in path_dic[a][sess].keys():
                    if fillNone:
                        if not v73:
                            mat = scipy.io.loadmat(path_dic[a][sess][sl])
                            dic[a][sess][sl] = {k: None for k in mat.keys()}
                            del mat
                        else:
                            mat = scipy.io.loadmat(path_dic[a][sess][sl])
                            dic[a][sess][sl] = {k: None for k in mat.keys()}
                            del mat
                    else:
                        if not v73:
                            dic[a][sess][sl] = scipy.io.loadmat(path_dic[a][sess][sl])
                        else:
                            dic[a][sess][sl] = mat73.loadmat(path_dic[a][sess][sl])
        self.dic = dic

    def same_empty(self):
        return {a:{sess:{slice:{k:None for k in self.dic[a][sess][slice].keys()}
                  for slice in self.dic[a][sess].keys()} for sess in self.dic[a].keys()} for a in self.dic.keys()}

    @classmethod
    def from_dic(self,dic):
        fS = fusSet()
        fS.dic = dic
        return fS

    def __getitem__(self,k):
        ## Obtain a subset of the tree dictionary from a list
        # use the key None to ask for all elements in the list
        if type(k)==list or type(k)==tuple:
            return fusSet.from_dic(dic_iterate(self.dic,k))
        else:
            if k==None:
                return self
            return fusSet.from_dic({k: self.dic[k]})

    def __setitem__(self, key, value):
        if type(key)==list or type(key)==tuple:
            dic_set(self.dic,list(key),value)
        else:
            raise Exception(" not well written")

    def keys(self):
        return self.dic.keys()

    def final_keys(self):
        # provide the keys in the final leaf of the structure
        leaf = next(iter(self))
        return leaf.keys()

    def get(self,keys):
        return dic_get(self.dic,keys)

    def __iter__(self):
        ## enables iterating the object
        # as in     for ? in x
        # or with   iter(x)
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
        sessions = list(self.dic[animals[0]].keys())
        slices = self.dic[animals[0]][sessions[0]].keys()
        nDic = {n: {sess: {slice: {k: None} for slice in slices} for sess in sessions}}
        fset = fusSet.from_dic(nDic)
        for sess in sessions:
            for slice in slices:
                to_stack = []
                for a in animals:
                    to_stack += [self.dic[a][sess][slice][k]]
                fset[n, sess, slice, k] = fmerge(to_stack, axis=axis)
        return fset
    def merge_session(self,ks,n:str,fmerge,axis):
        #k: the final key to use and where to apply the merge, can be a list in which case
        # the stacking is made for all element in the list
        #n: new name
        #fmerge: the function that makes the merge
        #axis: parameter to fmerge
        if type(ks)!=list:
            ks = [ks]

        animals = list(self.dic.keys())
        nDic = {a:None for a in animals}
        for a in animals:
            sessions = list(self.dic[a].keys())
            ## assumes a similar session across animals:
            slices = self.dic[a][sessions[0]].keys()
            nDic[a] = {n:{slice:{k:None for k in ks} for slice in slices}}
            for slice in slices:
                for k in ks:
                    to_stack = []
                    for sess in sessions:
                        to_stack += [self.dic[a][sess][slice][k]]
                    nDic[a][n][slice][k] = fmerge(to_stack, axis=axis)
        fset = fusSet.from_dic(nDic)
        return fset
    def merge_slice(self,ks,n:str,fmerge,axis):
        if type(ks)!=list:
            ks = [ks]

        animals = list(self.dic.keys())
        nDic = {a:None for a in animals}
        for a in animals:
            sessions = self.dic[a].keys()
            nDic[a] = {sess:{n:{k:None for k in ks}} for sess in sessions}
            for sess in sessions:
                for k in ks:
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


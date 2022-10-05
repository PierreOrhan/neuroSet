# Nested dictionaries of arbitrary dimensions
from data_drivers.data_drivers import data_driver
import numpy as np
import os

def _keys_to_name(keys):
    n = ""
    for k in keys[-1]:
        n = n + k + "_"
    n = n + keys[-1]
    return n

def _dickeyiter_rec(dic,level):
    ## returns tuple composed of the path to each element in the dictionary
    if level==0:
        yield ()
    else:
        for k in dic.keys():
            for dic_lower in _diciter_rec(dic[k],level-1):
                yield (k,)+dic_lower

def _diciter_rec(dic,level):
    if level==0:
        yield dic
    else:
        for k in dic.keys():
            for dic_lower in _diciter_rec(dic[k],level-1):
                yield dic_lower

def _dicCreate_rec(dic,new_dic,level):
    if level==0:
        return {}
    else:
        for k in dic.keys():
            new_dic[k] = _emptyDicCreate_rec(dic[k], {},level-1)
            return new_dic[k]

def _emptyDicCreate_rec(dic,new_dic,level):
    if level==0:
        return None
    else:
        for k in dic.keys():
            new_dic[k] = _emptyDicCreate_rec(dic[k], {},level-1)
            return new_dic[k]

def fzip(*args):
    ## zip multiple nested sets
    # and check that the keys of the set are matching!
    for sets_elem in zip(*[a.same_key() for a in args]):
        for s in sets_elem:
            try:
                assert np.all(s["key"] == sets_elem[0]["key"])
            except:
                raise Exception("two nested set do not output their file in the same order")
    for sets in zip(*args):
        yield  sets

class nSet():
    ## the idea of a nSet is to construct nested dictionaries with precise meaning associated
    # to each level.

    def __init__(self,num_level : int = 0):
        self.dic = {}
        self.num_level = num_level

    def load(self,dir,load_driver : data_driver,fillNone: bool = False,subsets=None):
        # dir: path to the directory

        # load_driver: a python object called to load the files or sub-directory found in the nSet directory

        # fillNone: only load the directory structure and the keys of each leaf element, but with empty elements
        # allows quick inspection of the dataset without too much memory overhead

        # subsets: list of strings: subsets of file to load. Load all if None
        self.load_driver = load_driver

        ldir = os.listdir(dir)
        if type(subsets) != type(None):
            ls = np.array(subsets)
            # verify that the subsets_file exist in the directory
            assert np.all([np.any([l==self.load_driver.remove_extension(ld) for ld in ldir]) for l in ls])
        elif len(ldir)==0:
            return None # empty nSet
        else:
            ls = np.array([self.load_driver.remove_extension(l) for l in os.listdir(dir)])
        splited_names = np.array([a.split("_") for a in ls])
        # verify that all files have the same number of "_" parameters so that we have consistent level across
        # the dataset.
        assert np.unique([len(a) for a in splited_names]).shape[0] == 1

        nb_level = len(splited_names[0])

        dic = {}
        for fname,key in zip(ls,splited_names):
            if not key[0] in dic.keys():
                dic[key[0]] = {}
            sub_dic = dic[key[0]]
            for level in np.arange(1,nb_level-1):
                if not key[level] in sub_dic.keys():
                    sub_dic[key[level]] = {}
                sub_dic = sub_dic[key[level]]
            # last level particularity: load the data
            sub_dic[key[-1]] = self.load_driver.load(os.path.join(dir,fname),fillNone=fillNone)
        self.dic = dic
        return None

    def keys(self):
        return self.dic.keys()

    def __iter__(self):
        # enables iterating the object
        # as in     for ? in x
        # or with   iter(x)
        ## --> this is effectively a recursive function
        return _diciter_rec(self.dic,self.num_level)

    def final_keys(self):
        # provide the keys in the final leaf of the structure
        leaf = next(iter(self))
        return leaf.keys()

    def get(self,keys):
        return dic_get(self.dic,keys)


    def save(self,path):
        ## saving tool: iterate
        keyset = self.same_key()
        for leaf,keys in zip(self,keyset):
            n = _keys_to_name(keys)
            target_path = os.path.join(path, n)
            self.load_driver.save(target_path,leaf)

    def _append(self,path):
        # append with no verification
        keyset = self.same_key()
        for leaf, keys in fzip(self, keyset):
            n = _keys_to_name(keys)
            target_path = os.path.join(path, n)
            self.load_driver.append(target_path, leaf)

    def append(self,path):
        # Appends to the nSet existing in path, the data in the leaf of this nSet
        # First verify that there is a nSet in the path
        # and that the nSet share the same level
        targetSet = nSet()
        targetSet.load(path, load_driver= self.load_driver,fillNone=True)

        assert targetSet.num_level == self.num_level
        ## verify through fzip that we can iterate across both sets together...
        for _ in fzip(targetSet,self):
            pass

        self._append(path)


    def append_sub(self,path):
        # Same as append but it allows for this nSet to be a sub-nSet of the target
        # meaning that all file in this nSet should be in the target nSet, but the target could be bigger...
        targetSet = nSet()
        targetSet.load(path, load_driver=self.load_driver, fillNone=True)
        assert targetSet.num_level == self.num_level
        fileTarget = targetSet.get_file_list()
        assert np.all([np.any([f==ftarget for ftarget in fileTarget]) for f in self.get_file_list()])

        self._append(path)


    def get_file_list(self):
        ## list all the putative file in a saving repository that would contain this fusset:
        list_file = []
        keyset = self.same_key()
        for keys in keyset:
            list_file+=[keys["key"][0]+"_"+keys["key"][1]+"_"+keys["key"][2]]
        return list_file

    def same_empty(self):
        # Provide a similar nested subset dictionary but with completely empty leaf
        new_dic = _emptyDicCreate_rec(self.dic, {},level)
        new_nSet = nSet.from_dic(new_dic,self.num_level)
        return new_nSet

    def same_key(self):
        # The leaf is a dictionnary with one element "key" and one value: the list of keys
        new_dic = _dicCreate_rec(self.dic, {},level)
        new_nSet = nSet.from_dic(new_dic,self.num_level)

        # iterator over the keys:
        key_iterator = _dickeyiter_rec(self.dic,self.num_level)

        for res,key in zip(new_nSet,key_iterator):
            res["key"] =  list(key)

        return new_nSet

    def c_yield(self,ks):
        # Conditional yielding
        # ks is a list of size the number of level containing either:
        #       None --> no condition at this level
        #       a list of allowed element of the level.
        # returns a generator that only yields the leaf elements respecting the criteria
        nset_key = self.same_key()
        for x,key in fzip(self,nset_key):
            is_allowed = True
            for idk,k in enumerate(key["key"]):
                if type(ks[idk]) != type(None) and not (k in key):
                    is_allowed = False
            if is_allowed:
                yield x

    def merge_level(self,level,ks,n:str,fmerge,axis):
        ## Merge all elements at one level by applying the function "fmerge"
        # Note: if the shape across leaf element changes the fmerge should do something about it.
        # Note: the nSet keeps to be a nSet with a constant number of level, this level is simply a dictionary
        # with one dic elements the sub-dictionary

        # ## assumes a similar substructure across animals:
        # if type(ks)!=list:
        #     ks = [ks]
        #
        # animals = list(self.dic.keys())
        # sessions = list(self.dic[animals[0]].keys())
        # slices = self.dic[animals[0]][sessions[0]].keys()
        # nDic = {n: {sess: {slice: {k:None for k in ks} for slice in slices} for sess in sessions}}
        # fset = fusSet.from_dic(nDic)
        # for k in ks:
        #     for sess in sessions:
        #         for slice in slices:
        #             to_stack = []
        #             for a in animals:
        #                 to_stack += [self.dic[a][sess][slice][k]]
        #             fset[n, sess, slice, k] = fmerge(to_stack, axis=axis)
        # return fset

    ### Some more complicated commands on the nSet:

    def __getitem__(self, k):
        ## Obtain a subset of the tree dictionary from a list
        # use the key None to ask for all elements in the list
        if type(k) == list or type(k) == tuple:
            return nSet.from_dic(dic_iterate(self.dic, k))
        else:
            if k == None:
                return self
            return nSet.from_dic({k: self.dic[k]})

    def __setitem__(self, key, value):
        if type(key) == list or type(key) == tuple:
            dic_set(self.dic, list(key), value)
        else:
            raise Exception(" not well written")

    ### class-method
    @classmethod
    def from_dic(self,dic,num_level):
        nS = nSet()
        nS.dic = dic
        nS.num_level = num_level
        return nS
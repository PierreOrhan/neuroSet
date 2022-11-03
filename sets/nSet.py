# Nested dictionaries of arbitrary dimensions
from data_drivers.data_drivers import data_driver
from sets.dic_util import *
import numpy as np
import os

def _keys_to_name(keys):
    n = ""
    for k in keys[:-1]:
        n = n + k + "_"
    n = n + keys[-1]
    return n

def _dickeyiter_rec(dic,level):
    ## returns tuple composed of the path to each element in the dictionary
    if level==0:
        yield ()
    else:
        for k in dic.keys():
            for dic_lower in _dickeyiter_rec(dic[k],level-1):
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
            new_dic[k] = _dicCreate_rec(dic[k], {},level-1)
        return new_dic

def _emptyDicCreate_rec(dic,new_dic,level):
    if level==0:
        return None
    else:
        for k in dic.keys():
            new_dic[k] = _emptyDicCreate_rec(dic[k], {},level-1)
        return new_dic

def fzip(*args):
    ## safely zip multiple nested sets
    # Make sure that there is the same number of file:
    # and check that the final keys of the set are matching!

    # test for number:
    file_lists = [a.get_file_list() for a in args]
    try:
        assert np.unique([len(f) for f in file_lists]).shape[0] == 1
    except:
        raise Exception("Can't safely zip: the nSets do not have the same structure")
    # Make sure that the file are output in the same exact order:
    for sets_elem in zip(*[a.same_key() for a in args]):
        for s in sets_elem:
            try:
                assert np.all(s["key"] == sets_elem[0]["key"])
            except:
                raise Exception("Can't safely zip: the nSets do not output their file in the same order")
    # returns the generator as we checked for the safety of it:
    for sets in zip(*args):
        yield  sets

from itertools import  product
def pzip(fzip1,fzip2):
    # Provided two fziped or two nSet, we iterate across each
    # and then iterate across the other
    # returns a tuple composed of the two leafs
    for k1,k2 in product(fzip1,fzip2):
        yield (k1,k2)


def product_dicempty(nSet1,nSet2):
    # returns an empty nSet that replaced the leaf of nSet1 with nSet2 initialized as an empty dic:
    nSet2_emptydic = nSet2.same_emptydic()
    newSet = nSet1.same_emptydic()
    newSet.num_level = newSet.num_level-1 # will iterate just above the last level...
    outSet = nSet1.same_emptydic()

    for n,k in fzip(newSet,newSet.same_key()):
        for lk in n.keys():
            call_key = [[u] for u in k["key"]]+[lk]
            outSet[call_key] = nSet2_emptydic.dic
    outSet.num_level = nSet1.num_level + nSet2.num_level #+1 do compensate the previous removal
    return outSet


class nSet():
    ## the idea of a nSet is to construct nested dictionaries with precise meaning associated
    # to each level.

    def __init__(self,num_level : int = 0):
        self.dic = {}
        self.num_level = num_level
        self.load_driver = None #to be defined...

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

        self.num_level = len(splited_names[0])

        if self.num_level>1:
            dic = {}
            for fname,key in zip(ls,splited_names):
                if not key[0] in dic.keys():
                    dic[key[0]] = {}
                sub_dic = dic[key[0]]
                for level in np.arange(1,self.num_level-1):
                    if not key[level] in sub_dic.keys():
                        sub_dic[key[level]] = {}
                    sub_dic = sub_dic[key[level]]
                # last level particularity: load the data
                sub_dic[key[-1]] = self.load_driver.load(os.path.join(dir,fname),fillNone=fillNone)
        elif self.num_level==1:
            dic = {ls[k]: self.load_driver.load(os.path.join(dir,ls[k]),fillNone=fillNone) for k in range(len(ls))}
        else:
            raise Exception("can't load an empty directory in an nSet")
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

    def save(self,path):
        ## saving tool: iterate
        keyset = self.same_key()
        for leaf,keys in zip(self,keyset):
            n = _keys_to_name(keys["key"])
            target_path = os.path.join(path, n)
            self.load_driver.save(target_path,leaf)

    def _append(self,path):
        # append with no verification
        keyset = self.same_key()
        for leaf, keys in fzip(self, keyset):
            n = _keys_to_name(keys["key"])
            target_path = os.path.join(path, n)
            self.load_driver.append(target_path, leaf)

    def append(self,path):

        # Appends to the nSet existing in path, the data in the leaf of this nSet
        # First verify that there is a nSet in the path
        # and that the nSet share the same exact levels
        targetSet = nSet()
        targetSet.load(path, self.load_driver,fillNone=True)

        assert targetSet.num_level == self.num_level
        ## verify through fzip that we can iterate across both sets together...
        for _ in fzip(targetSet,self):
            pass

        self._append(path)


    def append_sub(self,path):
        # Same as append but a bit less restrictive: it allows for this nSet to be a sub-nSet of the target
        # meaning that all file in this nSet should be in the target nSet, but the target could be bigger...
        targetSet = nSet()
        targetSet.load(path, self.load_driver, fillNone=True)
        assert targetSet.num_level == self.num_level
        fileTarget = targetSet.get_file_list()
        assert np.all([np.any([f==ftarget for ftarget in fileTarget]) for f in self.get_file_list()])

        self._append(path)


    def get_file_list(self):
        ## list all the putative file in a saving repository that would contain this fusset:
        list_file = []
        keyset = self.same_key()
        for keys in keyset:
            list_file+=[_keys_to_name(keys["key"])]
        return list_file

    def same_empty(self):
        # Provide a similar nested subset dictionary but with completely empty leaf
        new_dic = _emptyDicCreate_rec(self.dic, {},self.num_level)
        new_nSet = nSet.from_dic(new_dic,self.num_level)
        return new_nSet

    def same_emptydic(self):
        # The leaf is an empty dictionary
        new_dic = _dicCreate_rec(self.dic, {},self.num_level)
        new_nSet = nSet.from_dic(new_dic,self.num_level)
        return new_nSet

    def same_key(self):
        # The leaf is a dictionnary with one element "key" and one value: the list of keys
        new_nSet = self.same_emptydic()
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

    def merge_level(self,level,final_ks,output_name:str,fmerge,**kwargs):

        ## Merge all elements at one level by applying the function "fmerge"
        # This requires that sub-levels post that level have exactly the same structure.
        # --> we verify that by initializing new nSet with these sub-structures and verifying that we can fzip
        # them

        # level : integer, indicates the level at which we shoudl merge.
        #   level 0 is the root and the level==self.num_level-1 corresponds to the leaf
        # output_name is used to produce a new name for the merged level
        # fmerge: takes a list of leaf element and combine them

        # Note: if the shape across leaf element changes the fmerge should do something about it.
        # Note: the nSet keeps to be a nSet with a constant number of level, this level is simply a dictionary
        # with one dic elements the sub-dictionary

        assert level<= self.num_level-1
        assert np.all([np.any([f==k for f in self.final_keys()]) for k in final_ks])


        outNset = nSet.from_dic(_dicCreate_rec(self.dic, {},level),level)
        for new_subdic,subdic in zip(outNset,_diciter_rec(self.dic, level)):
            # for all indices in higher level: gather the dictionary at this level
            #level==0 will raise all root dics ...
            if level<self.num_level-1:
                to_merge = []
                for sublevel_dics in _diciter_rec(subdic,1):
                    to_merge += [nSet.from_dic(sublevel_dics,self.num_level-level-1)]
                # verify that the sub_nset have similar structure:
                # and iterate across the final leaf elements in the structure
                target_dic = to_merge[0].same_emptydic()
                for leaf_diclist in fzip(*(to_merge+[target_dic])):
                    # apply fmerge for all final keys:
                    for k in final_ks:
                        leaf_diclist[-1][k] = fmerge([l[k] for l in leaf_diclist[:-1]], **kwargs)
                ## we need to add the replace the subdic with the target_dic:
                new_subdic[output_name] = target_dic.dic
            else: # slightly different for the last level
                target_dic = {}
                for k in final_ks:
                    target_dic[k] = fmerge([subdic[l][k] for l in subdic.keys()], **kwargs)
                new_subdic[output_name] = target_dic
        # hack
        outNset.num_level = self.num_level
        return outNset

    def remove_level(self,level):
        # If a level has one element, this function allows to get rid of it
        # the num_level is consequently decreased by one.
        assert level<= self.num_level-1
        if level==0:
            assert  len(list(self.dic.keys()))==1
            b = list(self.dic.keys())[0]
            self.dic = {k:self.dic[b][k] for k in self.dic[b].keys()}
            self.num_level -= 1
            return self
        else:
            # for subdic in _diciter_rec(self.dic, level):
            #     assert len(list(subdic.keys()))==1
            #     bs = list(subdic.keys())
            #     new_dic = {k: subdic[bs[0]][k] for k in subdic[bs[0]].keys()}
            #     subdic.pop(bs[0])
            #     for k in new_dic.keys():
            #         subdic[k] = new_dic[k]
            for subdic in _diciter_rec(self.dic, level-1):
                assert np.all([len(list(subdic[k].keys()))==1 for k in subdic.keys()])
                for k in subdic.keys():
                    bs = list(subdic[k].keys())
                    subdic[k] = subdic[k][bs[0]]
            self.num_level -= 1
            return self

    ### Some more complicated commands on the nSet:

    def get(self, keys):
        return dic_get(self.dic, keys)

    def __getitem__(self, k):
        ## Obtain a subset of the tree dictionary from a list
        # use the key None to ask for all elements in the list
        if type(k) == list or type(k) == tuple:
            newSet = nSet.from_dic(dic_iterate(self.dic, k),self.num_level)
            newSet.load_driver = self.load_driver
            return newSet
        else:
            if k == None:
                return self
            newSet = nSet.from_dic({k: self.dic[k]},self.num_level)
            newSet.load_driver = self.load_driver
            return newSet

    def __setitem__(self, key, value):
        if type(key) == list or type(key) == tuple:
            dic_set(self.dic, list(key), value)
        else:
            raise Exception(" not well written")

    def leaf_filter(self,leaf_list):
        # return a new nSet containing only the leaf present in leaf_list.
        splited_names = np.array([a.split("_") for a in leaf_list])
        assert np.all(np.any([a==f for f in self.get_file_list()]) for a in leaf_list)
        # verify that all files have the same number of "_" parameters so that we have consistent level across
        # the dataset.
        assert np.unique([len(a) for a in splited_names]).shape[0] == 1
        if self.num_level > 1:
            dic = {}
            for fname, key in zip(leaf_list, splited_names):
                if not key[0] in dic.keys():
                    dic[key[0]] = {}
                sub_dic = dic[key[0]]
                for level in np.arange(1, self.num_level - 1):
                    if not key[level] in sub_dic.keys():
                        sub_dic[key[level]] = {}
                    sub_dic = sub_dic[key[level]]
                # last level particularity: load the data
                sub_dic[key[-1]] = self.get(key)
        elif self.num_level == 1:
            dic = {leaf_list[k]: self.get(splited_names[k]) for k in range(len(leaf_list))}
        else:
            raise Exception("can't load an empty directory in an nSet")
        b = nSet()
        b.num_level = self.num_level
        b.dic = dic
        return b




    ### class-method
    @classmethod
    def from_dic(self,dic,num_level):
        # Instantiate a nSet from an existing dictionary,
        # one should set the data_driver for future loading and saving.

        nS = nSet()
        nS.dic = dic
        nS.num_level = num_level
        return nS




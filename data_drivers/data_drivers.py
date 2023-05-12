from abc import ABC, abstractmethod
import hdf5storage

class data_driver(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def remove_extension(self,path: str):
        # should remove the extension used by the data_drivers if any...
        pass

    @abstractmethod
    def load(self,path : str,fillNone: bool):
        pass

    @abstractmethod
    def save(self,path : str,leaf_data):
        pass

    @abstractmethod
    def append(self,path : str,leaf_data):
        pass

    @abstractmethod
    def filter_metadata(self,dir: list[str]):
        # from the list of files in a dir, filter the ones that are metadata for the storing driver.
        pass

class hdf5_driver(data_driver):
    ## This driver is inefficient because we load all the data even with the None argument
    def __init__(self):
        pass
    def remove_extension(self,path: str):
        return path.split(".mat")[0]
    def load(self,path : str,fillNone: bool):
        mat = hdf5storage.loadmat(path)
        if fillNone:
            dic = {k: None for k in mat.keys()}
            del mat
            return dic
        return mat
    def save(self,path :str,leaf_data):
        hdf5storage.savemat(path,leaf_data)

    def append(self,path : str,leaf_data):
        raise Exception("can't append online with hdf5_driver")

    def filter_metadata(self, dir: list[str]):
        return dir


## TODO: depending on the driver new possibility might arise, like to loop_up for the shape
# and for the axis names. It would be good to have those offered by the nSet...

import zarr as zr

class zarr_driver(data_driver):
    # Uses the power of the zarr array to load without directly charging in memory the data
    # In the resulting nSet, each leaf is a zarr group.

    def __init__(self):
        pass
    def remove_extension(self,path : str):
        # directory like files...
        return path
    def load(self,path :str, fillNone: bool):
        if fillNone:
            # open the group
            z = zr.open_group(path,mode="r")
            z2 = zr.group()
            for k in z.keys():
                z2.array(k,[])
            return z2
        else:
            return zr.open(path,mode="r")
    def save(self,path: str,leaf_data):
        z = zr.open_group(path,mode="w")
        for k in leaf_data.keys():
            z.array(name = k,data = leaf_data[k])

    def append(self,path:str,leaf_data):
        # Note this appending generate multiple arrays from the leaf_data into the existing zarr group
        # and therefore do not append or store the result inside existing arrays
        z = zr.open_group(path,mode="a")
        for k in leaf_data.keys():
            z.array(name = k,data = leaf_data[k])

    def filter_metadata(self, dir: list[str]):
        return dir

# import zarr as zr
# def nset_from_zg(group_dir):
#     # convert a zarr group into a nSet
#     zg = zr.open_group(group_dir, mode="r")
#     subset = list(zg.keys())
#     try:
#         assert np.unique([len(s.split("_")) for s in zg.key()])
#     except:
#         raise Exception("zarr group does not have names of the type xx_yy_zz with a fixed number of _")
#     netSet = nSet(num_level=len(subset[0].split("_")))
#     netSet.load(group_dir,
#                 load_driver=zarr_group_driver(),
#                 subsets=subset)
#     return netSet

import os
from os import path

class zarr_group_driver(data_driver):
    # If a zarr group contains elements that are structured with the "_"
    # format, this method allows to load them

    # In the resulting nSet the globel nSet is the original zarr group
    # and each leaf is a zarray.
    # The leaf will contain only one element with name "data" (unless otherwise precised)

    def __init__(self):
        pass
    def remove_extension(self,path : str):
        # directory like files...
        return path
    def load(self,path :str, fillNone: bool,leaf_name:str = "data"):
        # group_dir = os.path.join(*[p for p in path.split("/")[:-1]])
        group_dir = os.path.join(path,"..")
        if fillNone:
            # open the group
            # z = zr.open_group(group_dir,mode="a")
            z2 = zr.group()
            z2.array(leaf_name,[])
            return z2
        else:
            zg = zr.open_group(group_dir,mode="r")
            return {leaf_name:zg[path.split("/")[-1]]}

    def filter_metadata(self,dir: list[str]):
        return list(filter(lambda d: not d.__contains__(".zgroup"),dir))

    def save(self,path: str,leaf_data):
        raise Exception("not implemented error")
    def append(self,path:str,leaf_data):
        raise Exception("not implemented error")

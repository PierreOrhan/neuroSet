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


## TODO: depending on the driver new possibility might arise, like to loop_up for the shape
# and for the axis names. It would be good to have those offered by the nSet...

import zarr as zr

class zarr_driver(data_driver):
    # Uses the power of the zarr array to load without directly charging in memory the data

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
        z = zr.open_group(path,mode="a")
        for k in leaf_data.keys():
            z.array(name = k,data = leaf_data[k])

    def append(self,path:str,leaf_data):
        z = zr.open_group(path,mode="a")
        for k in leaf_data.keys():
            z.array(name = k,data = leaf_data[k])

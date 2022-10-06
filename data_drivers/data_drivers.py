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

class zarr_driver():
    ## This driver does not use the power of zarr array and is just used
    # to save and load quickly the leaf data as if they were some numpy array

    ## TODO: rethink this driver
    def __init__(self):
        pass

    def remove_extension(self,path : str):
        # directory like files...
        pass

    def load(self,path :str, fillNone: bool):
        if fillNone:
            # open the group
            z = zr.open(path)
            # outout empty_like:
            z2 = z.empty_like()
            return z

    def save(self,path: str,leaf_data):
        z = zr.open_group(path)
        for k in leaf_data.keys():
            z.array(path)


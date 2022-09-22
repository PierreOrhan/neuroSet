import numpy as np
import cv2

def matlab_mask(mask,x):
    # x: one-dimensional array organized in matlab order ("F", ie outside loop are the last dimensions)
    # mask: 2d mask
    mask_flat = mask.reshape((-1), order="F")  # to matlab style
    if len(x.shape)==0:
        x_full = np.zeros(mask.shape).reshape((-1),order="F") + np.nan
        x_full[mask_flat] = x
        x_image = np.reshape(x_full, mask.shape, order="F")
    else:
        x_full = np.zeros(x.shape[:-1]+mask.shape).reshape(x.shape[:-1]+(-1,),order="F") + np.nan
        x_full[...,mask_flat] = x
        x_image = np.reshape(x_full,x.shape[:-1]+mask.shape, order="F")
    return x_image

def matlab_mask_without(mask,x,out):
    # x: one-dimensional array organized in matlab order ("F", ie outside loop are the last dimensions)
    # out: allready initialized out matrix
    # mask: 2d mask
    mask_flat = mask.reshape((-1), order="F")  # to matlab style
    if len(x.shape)==0:
        x_full = out.reshape((-1),order="F")
        x_full[mask_flat] = x
        x_image = np.reshape(x_full, mask.shape, order="F")
    else:
        x_full = out.reshape(x.shape[:-1]+(-1,),order="F")
        x_full[...,mask_flat] = x
        x_image = np.reshape(x_full,x.shape[:-1]+mask.shape, order="F")
    return x_image


### tools to get the upper and lower cortical surface of anatomical slices
import matplotlib.pyplot as plt
def get_surface(anat):
    # anat: image with mean activity over session and repetition

    cm = plt.get_cmap("gray")
    fig,ax = plt.subplots()
    ax.matshow(np.log(anat),cmap=cm)
    fig.show()
    ax.set_title("please click on outer surface, in order from left to right")
    res = plt.ginput(n=1000,timeout=-1, show_clicks=True)
    res2 = np.stack(res)
    ax.set_title("please click on inner surface, in order from left to right")
    res_inner = plt.ginput(n=1000,timeout=-1, show_clicks=True)
    res2_inner = np.stack(res_inner)

    ## Finally we compute the surface as a segmented line over the cortex
    for idp,p in enumerate(res[:-1]):
        # the rule for wiring is simple:
        # we focus on the matrix of voxels between two clicked voxel
        # and light up the diagonal of this matrix, and then the straight line to the end
        cv2.drawLine(polylines,)




    return res2,res2_inner










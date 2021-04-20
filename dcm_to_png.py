import warnings
warnings.filterwarnings("ignore")

import os
import cv2
import glob
import time
import pydicom
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

import dask as dd
import dask.array as da
from dask.distributed import Client, progress

print(os.listdir("/home/mabbutrishulreddy/Downloads/dcm_images/stage_2_test_images"))

data_dir = Path('/home/mabbutrishulreddy/Downloads/dcm_images/stage_2_test_images/')

# get the list of all the dcm files recursively
all_files = list(data_dir.glob("**/*.dcm"))

print("Number of dcm files found: ", len(all_files))

outdir = "/home/mabbutrishulreddy/Downloads/png_dicom_images/"

# Make the directory
if not os.path.exists(outdir):
    os.mkdir(outdir)

def convert_images(filename, img_type='png'):
    """Reads a dcm file and saves the files as png/jpg
    
    Args:
        filename: path to the dcm file
        img_type: format of the processed file (jpg or png)
        
    """
    # extract the name of the file
    name = filename.parts[-1]
    
    # read the dcm file
    ds = pydicom.read_file(str(filename)) 
    img = ds.pixel_array
    
    # save the image as jpg/png
    if img_type=="jpg":
        cv2.imwrite(outdir + name.replace('.dcm','.jpg'), img)
    else:
        cv2.imwrite(outdir + name.replace('.dcm','.png'), img)

all_files = all_files*1
print("Total number of files: ", len(all_files))
t = time.time()
for f in all_files:
    convert_images(f)
print("Time taken : ", time.time() - t)
all_images = [dd.delayed(convert_images)(all_files[x]) for x in range(len(all_files))]

t = time.time()
dd.compute(all_images)
print("Time taken when using all cores: ", time.time()-t)


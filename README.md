# operational-processing
Scripts to run operational CloudnetPy processing

# Installation
```
$ git clone git@github.com:actris-cloudnet/operational-processing.git
$ cd operational-processing
$ python3 -m venv venv
$ source venv/bin/activate
(venv) $ pip3 install --upgrade pip
(venv) $ pip3 install .
```

## Concatenating CHM15k ceilometer files
Some of the CHM15k lidar files come in several files per day while ```cloundetPy``` 
processing requires daily files. A script can be used to generate these daily files. 
### Usage
Launch from the root folder:
```
$ scripts/concat-lidar.py <input_folder>
```
where ```<input_folder> ``` is the root level folder storing the original ceilometer files in 
```input_folder/year/month/day/*.nc``` structure.

Optional arguments:
*  ```-d, --dir <dir_name> ``` Separate folder for the daily files.
* ``` -o, --overwrite True``` Overwrite any existing daily files.
* ``` --year YYYY``` Limit to certain year only.
* ``` --month MM``` Limit to certain month only.
* ``` --day DD``` Limit to certain day only.
* ``` -l --limit N``` Run only on folders modified within ```N``` hours . Forces overwriting of daily files.

### Examples
```
$ scripts/concat-lidar.py /data/bucharest/uncalibrated/chm15k/ 
```
This will concatenate, for example, ```/data/bucharest/uncalibrated/chm15k/2020/01/15/*.nc``` into 
```/data/bucharest/uncalibrated/chm15k/2020/chm15k_20200115.nc```, and so on.

After the initial concatenation for all existing folders has been performed, 
it is usually sufficient to use the ```-l``` switch:

```
$ scripts/concat-lidar.py /data/bucharest/uncalibrated/chm15k/ -l=24
```
Which finds the folders updated within 24 hours and overrides daily files from these folders making sure 
they are always up to date (if the script is run daily).

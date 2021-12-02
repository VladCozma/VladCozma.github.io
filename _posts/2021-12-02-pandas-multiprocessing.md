---
layout: post
title: How to run a multi process panda apply function
comments: true
description: "How to run a multi process panda apply function"
tags: [pandas, multiprocessing, python]
related_posts: [Getting started with Machine Learning]
---

### Problem
Having a (very) large dataframe, one would like to apply a function to one of the columns. To speed up the execution the fuction could be executed in parallel over multiple processes.  


#### Proposal
Use multiprocessing to split the work on cpu's.

One way of doing that is using python's **multiprocessing** library.

#### Description

Using **multiprocessing** library you can create a pool of processes, each of them getting a chunk of work to execute. The processes can be spawned in [3 different ways](https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods). Assuming there is a context that needs to be shared with every process (like a python virtual environment), you should **fork** the main process. You do that at import:
``` python
import multiprocessing as mp
mp.set_start_method("fork")
```
There is, of course, complete documentation of [multiprocessing](https://docs.python.org/3/library/multiprocessing.html), so I'll go directly into implementation. To be able to start splitting processes, you need to create a **Pool** object from **mp**, which will create the, well, pool of processes that can start handling your work.
```python
POOLSIZE = mp.cpu_count()
with mp.Pool(POOLSIZE) as pool:
```
The number of processes is defined by the **POOLSIZE**, which normally should be the number of available CPU's which you get with **mp.cpu_count()**.
The **pool** object has a number of methods that you can summon. To apply a function to a dataframe, **map** is a suitable method, and you can do:
```python
pool.map(my_function, df[content_col_name])
```
If **my_function** requires fixed keyword arguments, you can pass those either by creating an iterator of the same size as **df**, or use **partial** function tool to map them:
```python
from functools import partial
mapfunc = partial(
    my_function,
    type="1",
    arg2=1,
)
```
**mapfunc(text)** is now equivalent with **my_function(text, type="1", arg2=1)**.

This **pool.map()** execution with split the dataframe in elements of 1 and send each to an available resource, and then collect back the results. To do this there is a quite large processing overhead (PO), so you need a good balance between the benefits of multiprocessing and the work needed to process the results. Therefore a **CHUNKSIZE** is recommended, to split the dataframe in manageable chunks. The choice of CHUNKSIZE is sometimes 'dark arts', but I'm using a 'naive' method of splitting the dataframe equally to the number of CPU's, and then splitting with a factor to normalize any unbalances in the dataframe. Also, a minimum **CHUNKSIZE** is recommended, in case you dataframe is not large enough. You can then apply the map with this chunk.
```python
MIN_CHUNK = 250
POOL_FACTOR = 12

POOLSIZE = mp.cpu_count()
DF_SIZE = df.shape[0]
CHUNKSIZE = max(MIN_CHUNK, int(DF_SIZE / POOLSIZE / POOL_FACTOR))
with mp.Pool(POOLSIZE) as pool:
    df[content_col_name] = pool.map(my_function, df[content_col_name]), chunksize=CHUNKSIZE)
```
In one of my usecases I chose the above figures, but trial and error could get much better results.
You will observer that in this case you have no insight in the progress of the execution. You can produce a progress bar with **tqdm** by using **imap** instead of **map**.
```python
with mp.Pool(POOLSIZE) as pool:
    df[content_col_name] = list(
                tqdm.tqdm(
                    pool.imap(
                        mapfunc, df[content_col_name], chunksize=CHUNKSIZE
                    ),
                    total=DF_SIZE,
                )
            )
```

#### Whole code

``` python
import pandas as pd
import tqdm
import multiprocessing as mp

from functools import partial
from sys import platform

mp.set_start_method("fork")

MIN_CHUNK = 250
POOL_FACTOR = 12

def my_function(
    text: str,
    type: str,
    arg2: int,
    ):
    return text 


def main():

    # creating a DataFrame   (stolen from internet)
    dictionary = {'Names':['Simon', 'Josh', 'Amen', 'Habby',
                        'Jonathan', 'Nick', 'Jake'],
                'Countries':['AUSTRIA', 'BELGIUM', 'BRAZIL',
                            'JAPAN', 'FRANCE', 'INDIA', 'GERMANY'],
                'Boolean':[True, False, False, True,
                            True, False, True],
                'HouseNo':[231, 453, 723, 924, 784, 561, 403],
                'Location':[12.34, 45.67, 03.45, 17.23,
                            83.12, 90.45, 84.34]}
    df = pd.DataFrame(dictionary, columns = ['Names', 'Countries',
                                                'Boolean', 'HouseNo', 'Location'])
    content_col_name = "Names"

    run_parallel = True

    if platform not in ["linux", "darwin"]:
        print(
            f"Multiprocess not available on platform:{platform}. Only ['linux','darwin'] available. "
        )
        run_parallel = False

    if run_parallel:
        # add kwargs to predict_single
        mapfunc = partial(
            my_function,
            type="1",
            arg2=1,
        )

        POOLSIZE = mp.cpu_count()
        DF_SIZE = df.shape[0]
        CHUNKSIZE = max(MIN_CHUNK, int(DF_SIZE / POOLSIZE / POOL_FACTOR))
        print("Parallel prediction started.")
        print(f"Poolsize: {POOLSIZE}")
        print(f"Chunksize: {CHUNKSIZE}")
        with mp.Pool(POOLSIZE) as pool:
            df["prediction"] = list(
                tqdm.tqdm(
                    pool.imap(
                        mapfunc, df[content_col_name], chunksize=CHUNKSIZE
                    ),
                    total=DF_SIZE,
                )
            )
        pool.close()

    else:
        tqdm.tqdm.pandas()
        df[content_col_name] = df[content_col_name].progress_apply(
            my_function,
            type="1",
            arg2=1,
        )

if __name__ == "__main__":
    main()

```




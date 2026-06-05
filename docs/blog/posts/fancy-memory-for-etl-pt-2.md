---
title: Fancy Memory for ETL pt. 2
date: 2026-06-05
author: Benjamin Zaitlen
slug: fancy-memory-for-etl-pt-2
draft: true
---

# Fancy Memory for ETL pt. 2

Let's dive a little deeper into what's happening with pinned and pageable memory from the previous post.  I took some nsys profiles with data and scripts developed in the last post.  

> nsys profile -o pageable-spill -f true  -t cuda,nvtx --stats=false python script.py

As a reminder, I measured the same join workflow which requires more VRAM than an L40 has (44GB) using two different memory types `regular/pageable host memory: 27s` and `pinned host memory: 23s`.


![Trace with pageable (non-pinned) memory](fancy-memory-for-etl-pt-2/full-nsys-pageable.png)
*Fig 1. Nsight Systems timeline showing the pageable-memory cudf-polars join workflow.*

![Trace with pinned memory](fancy-memory-for-etl-pt-2/full-nsys-pinned.png)
*Fig 2. Nsight Systems timeline showing the pinned-memory cudf-polars join workflow.*


Having these two images laid out together we can see a easily visually see similarities and differneces:

1. Both have multiple cudf_polars streams (blue bars) though Fig 1. has 6 and Fig 2. has 5 -- let's come back to that
1. With pageable memory (fig 1) we see a load of red and green bars and it starts near the 2sec mark
1. With pinned memory (fig 2), we have some different colors but the bars aren't nearly as wide. Also, the time starts near the 18sec mark. 
This 18s is time to allocate all that pinned memory

Again, from the previous post we also have how long we spent spilling (device-to-host) and unspilling (host-to-device) for both memory types.


| Mode | Direction | Time |
| --- | --- | ---: |
| Pageable host spilling | Device -> host | 8.06 s |
| Pageable host spilling | Host -> device | 4.06 s |
| Pinned host spilling | Device -> pinned host | 2.64 s |
| Pinned host spilling | Pinned host -> device | 3.15 s |

It's intersting, the pageable host spilling is slower when moving data Device->Host compared with Host->Device, however, when using pinned memory the time is very close.  Why ?  Zooming into a region where we are spilling can help inform our understanding

| Pageable device -> host | Pageable host -> device |
| --- | --- |
| ![Pageable device to host transfer detail](fancy-memory-for-etl-pt-2/page-DtoH.png) | ![Pageable host to device transfer detail](fancy-memory-for-etl-pt-2/page-HtoD.png) |

NSYS let's us easily see not just how much time was spent spilling, but also how much data was transfered, and Data/time gives us a througput measurement.  The following images are samples, each transfer will have some noise, but are very representative.  In the zoomed-in images, Device->Host, is 8.6GiB/s and Host->Device is ~17 GiB/s.  When transferring data using regular pageable memory, the host has to first allocate memory before the device can spill and it's apparently quite costly to this.  When moving data *back* to the device, there is not paging in host memory so it's significnatly faster.

> cudf-polars and RapidsMPF use stream ordered memory as a default: [cudaMallocAsync](https://docs.nvidia.com/cuda/cuda-runtime-api/group__CUDART__MEMORY__POOLS.html#group__CUDART__MEMORY__POOLS_1gbbf70065888d61853c047513baa14081) but let's not get into this now

Let's do the same kind of inspection of the pinned memory nsys plot:

| Pinned device -> host | Pinned host -> device |
| --- | --- |
| ![Pinned host to device transfer detail](fancy-memory-for-etl-pt-2/pinned-HtoD.png) | ![Pinned device to host transfer detail](fancy-memory-for-etl-pt-2/pinned-DtoH.png) |

Device->Host data movement is a lot faster: 24 GiB/s and Host->Device is 22GiB/s. Pinned memory throughput is so much faster because we paid all the memory host allocation fees during initalization.  The host doesn't have to allocate any memory -- it's already there for the device to use! 

Why though is the Host->Device faster for pinned memory compared with pageable memory ? It's great fun getting an exuse to learn about how machines actually work.  We aren't going to dive very deep, but just peer into the depths wihtout falling in.  When the host allocate pageable memory, the operating systems is still laregely in control of that memory and running the entire machine!  The OS can move the memory to another location or even swap it to disk.  This means the host, the OS, is responsible for moving the data ultimately and safeguarding the memory from corruption during the process.   It's safe but slow, and one of the primary reasons why [Direct Memory Access (DMA)](https://en.wikipedia.org/wiki/Direct_memory_access) was created and dates back to computing in the 50s when everything was built by standards committes



....
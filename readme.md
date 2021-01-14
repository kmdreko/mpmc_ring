# MPMC Ring

This library provides a [`Ring`](https://github.com/kmdreko/mpmc_ring/blob/master/src/lib.rs#L56) 
struct which allows for reading and writing of a ring-buffer/capped-queue of
elements with multiple readers and multiple writers concurrently.

A blog article recounting the development process and overall design can be
found at [kmdreko.github.io](https://kmdreko.github.io/posts/20191003/a-simple-lock-free-ring-buffer/).

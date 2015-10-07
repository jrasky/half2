# half2
This was originally going to be a sort of version control system, but I got sidetracked and instead just implemented a b-tree.

It's a decently performant implementation, which is just a little bit slower than the native Rust implementation. I think I know how I would make it as fast, which would involve using separate nodes and pointers instead of calculating offsets every time.

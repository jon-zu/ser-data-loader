# Ser-Data-Loader

A small serde based data loader to load data from de/serializable types concurrently on application startup. There's also a cache which can cache processed data(mapped data) so you can initially load a file, process It and only when the source file changes It will reload the source file and process It again.

# Notes

This crate utilizes the serde crate and uses bincode to cache mapped/transformed data. For now only JSON and Bincode are supported out of the box, but you can extend It easily via the `DataFormatHandler` trait. Also the executor logic is only implemented for `tokio` for now.


# Example

```rust
// Create a dataloader in the path
let mut loader = AutoDataLoader::new(&path).unwrap();

// Load simple json string
let txt = loader.load_file::<String>("a.json");
// Load and map the file
let mapped = loader.load_map("a.json", data_mapper_fn(|s: String| Ok(s.len())));
// Load and map, but cache the result so only on If `a.json` gets changed It maps It again
let mapped_cached = loader.load_map_cached(
    "a.json",
    data_mapper_fn(move |s: String| { Ok(s.len()) }),
);
// Wait for all data to load
loader.wait_all().await.unwrap();

// Access the data via get
assert_eq!(txt.get(), inp);
assert_eq!(mapped.get(), inp.len());
assert_eq!(mapped_cached.get(), inp.len());
```
use std::{
    collections::BTreeMap,
    fs::File,
    future::Future,
    io::{BufRead, BufReader, BufWriter, Read},
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc, RwLock},
    time::SystemTime,
};

use anyhow::Context;
use futures::{channel::oneshot, stream::FuturesUnordered, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Default)]
struct LoaderTaskSet {
    #[cfg(feature = "tokio")]
    inner: tokio::task::JoinSet<anyhow::Result<()>>,
}

impl LoaderTaskSet {
    fn spawn_task<F: FnOnce() -> anyhow::Result<()> + Send + 'static>(&mut self, f: F) {
        #[cfg(feature = "tokio")]
        {
            self.inner.spawn_blocking(f);
        }
    }

    async fn wait_all(self) -> anyhow::Result<()> {
        #[cfg(feature = "tokio")]
        {
            let res = self.inner.join_all().await;
            for res in res {
                res?;
            }
            Ok(())
        }
    }
}

/// Data format to read and load data from a file
pub trait DataFormat {
    /// Read data from a reader
    fn read_from<T: Data, R: Read + BufRead>(rdr: R) -> anyhow::Result<T>;

    /// Read data from a file
    fn read_from_file<T: Data>(path: &Path) -> anyhow::Result<T> {
        let file = File::open(path).context("open file")?;
        let rdr = BufReader::new(file);
        Self::read_from(rdr)
    }

    /// Write data to a writer
    fn write_to<T: Serialize, W: std::io::Write>(data: &T, wtr: W) -> anyhow::Result<()>;
    /// Write data to a file
    fn write_to_file<T: Serialize>(data: &T, path: &Path) -> anyhow::Result<()> {
        let file = File::create(path).context("create file")?;
        Self::write_to(data, file)
    }
}

/// JSON data format
pub struct JsonDataFormat;
impl DataFormat for JsonDataFormat {
    fn read_from<T: Data, R: Read + BufRead>(rdr: R) -> anyhow::Result<T> {
        serde_json::from_reader(rdr).context("parse json")
    }

    fn write_to<T: Serialize, W: std::io::Write>(data: &T, wtr: W) -> anyhow::Result<()> {
        serde_json::to_writer(wtr, data).context("write json")
    }
}

/// Bincode data format
pub struct BinCodeDataFormat;
impl DataFormat for BinCodeDataFormat {
    fn read_from<T: Data, R: Read + BufRead>(rdr: R) -> anyhow::Result<T> {
        bincode::deserialize_from(rdr).context("parse bincode")
    }

    fn write_to<T: Serialize, W: std::io::Write>(data: &T, wtr: W) -> anyhow::Result<()> {
        bincode::serialize_into(wtr, data).context("write bincode")
    }
}

/// Data mapper, to map data from an `In` type to an `Out` type,
/// this is useful for using the built-in caching mechanism of the `DataLoader`
pub trait DataMapper: Send + 'static {
    type In: Data;
    type Out: Send + 'static;

    fn map(self, data: Self::In) -> anyhow::Result<Self::Out>;
}

/// Create a data mapper from a function
pub fn data_mapper_fn<F, In, Out>(f: F) -> MapperFn<F, In, Out> {
    MapperFn::new(f)
}

/// Data mapper from a function
pub struct MapperFn<F, In, Out> {
    f: F,
    _t: std::marker::PhantomData<(In, Out)>,
}

impl<F, In, Out> Clone for MapperFn<F, In, Out>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            _t: std::marker::PhantomData,
        }
    }
}

impl<F, In, Out> MapperFn<F, In, Out> {
    /// Create a new data mapper from a function
    pub fn new(f: F) -> Self {
        Self {
            f,
            _t: std::marker::PhantomData,
        }
    }
}

impl<F, In: Data, Out: Send + 'static> DataMapper for MapperFn<F, In, Out>
where
    F: FnOnce(In) -> anyhow::Result<Out> + Send + 'static,
{
    type In = In;
    type Out = Out;

    fn map(self, data: Self::In) -> anyhow::Result<Self::Out> {
        (self.f)(data)
    }
}

/// Data trait, to mark a type as a data type
/// basically `DeserializeOwned + Send + 'static`
pub trait Data: DeserializeOwned + Send + 'static {}
impl<T: DeserializeOwned + Send + 'static> Data for T {}

pub struct DataReceiver<T>(oneshot::Receiver<T>);

impl<T> DataReceiver<T> {
    /// Get the data, this will usually wailt until the data is loaded
    pub fn get(mut self) -> T {
        self.0
            .try_recv()
            .expect("Data recv closed")
            .expect("Data recv no value")
    }
}

impl<T> Future for DataReceiver<T> {
    type Output = T;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(cx).map(Result::unwrap)
    }
}

/// Data format handler, to handle different data formats
pub trait DataFormatHandler {
    /// Load data from a file
    fn load_from_file<T: Data>(p: &Path) -> anyhow::Result<T>;
}

/// Auto data format handler, to automatically detects the data format from the file extension
pub struct AutoDataFormatHandler;
impl DataFormatHandler for AutoDataFormatHandler {
    fn load_from_file<T: Data>(p: &Path) -> anyhow::Result<T> {
        let ext = p.extension().context("no extension")?;
        match ext.to_string_lossy().to_lowercase().as_str() {
            "json" => JsonDataFormat::read_from_file(p),
            "bincode" => BinCodeDataFormat::read_from_file(p),
            _ => anyhow::bail!("unknown extension: {:?}", ext),
        }
    }
}

/// Entry for the Manifest for the cache
#[derive(Debug, Deserialize, Serialize, Clone)]
struct DataManifestEntry {
    last_changed: SystemTime,
    cached_name: String,
}

/// Manifest for the cache
#[derive(Debug, Deserialize, Serialize)]
struct DataManifest {
    pub entries: BTreeMap<PathBuf, DataManifestEntry>,
    pub counter: u64,
}

/// Cache for the data loader
struct Cache {
    manifest: RwLock<DataManifest>,
    dir: PathBuf,
    counter: AtomicU64,
}

impl Cache {
    /// Load the cache from a directory
    fn load(dir: &Path) -> anyhow::Result<Self> {
        if !dir.exists() {
            std::fs::create_dir(dir).context("create cache dir")?;
        }

        let manifest_path = dir.join("manifest.json");
        let manifest = if manifest_path.exists() {
            JsonDataFormat::read_from_file(&manifest_path)?
        } else {
            DataManifest {
                entries: BTreeMap::new(),
                counter: 0,
            }
        };

        let counter = manifest.counter;

        Ok(Self {
            manifest: RwLock::new(manifest),
            dir: dir.to_owned(),
            counter: counter.into(),
        })
    }

    /// Save the cache
    fn save(&self) -> anyhow::Result<()> {
        let mut manifest = self.manifest.write().expect("write");
        manifest.counter = self.counter.load(std::sync::atomic::Ordering::Relaxed);

        let manifest_path = self.dir.join("manifest.json");
        JsonDataFormat::write_to_file::<DataManifest>(&manifest, &manifest_path)
    }

    /// Update an entry in the cache
    fn update_entry<F>(&self, path: &Path, update_cached: F) -> anyhow::Result<()>
    where
        F: FnOnce(&mut BufWriter<File>) -> anyhow::Result<()>,
    {
        let path = path.canonicalize().expect("canonicalize path");
        let filename = path.file_name().expect("file_name").to_string_lossy();
        let num = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let cache_file = format!("{num}_{filename}.cache");

        let file = File::create(self.dir.join(&cache_file))?;
        let mut file = BufWriter::new(file);
        update_cached(&mut file)?;

        // Remove the old file
        if let Some(old) = self.get_entry(&path) {
            let old_cache = self.dir.join(&old.cached_name);
            std::fs::remove_file(old_cache).context("remove old cache file")?;
        }

        let last_changed = path
            .metadata()
            .context("metadata")?
            .modified()
            .expect("modified");
        self.manifest.write().expect("write").entries.insert(
            path.to_owned(),
            DataManifestEntry {
                last_changed,
                cached_name: cache_file,
            },
        );

        Ok(())
    }

    /// Get or update a data from the cache
    fn get_or_update<T: Data + Serialize>(
        &self,
        path: &Path,
        load: impl FnOnce(&Path) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        if let Some(entry) = self.get_validated_entry(path)? {
            let cache_file = self.dir.join(&entry.cached_name);
            let rdr = BufReader::new(File::open(cache_file).context("open cache file")?);
            return BinCodeDataFormat::read_from(rdr);
        }

        let data = load(path)?;
        self.update_entry(path, |file| BinCodeDataFormat::write_to(&data, file))?;
        Ok(data)
    }

    /// Get an entry from the cache
    fn get_entry(&self, path: &Path) -> Option<DataManifestEntry> {
        let norm = path.canonicalize().expect("canonicalize path");
        self.manifest
            .read()
            .expect("read")
            .entries
            .get(&norm)
            .cloned()
    }

    /// Get a validated entry from the cache
    fn get_validated_entry(&self, path: &Path) -> anyhow::Result<Option<DataManifestEntry>> {
        Ok(match self.get_entry(path) {
            Some(entry) => {
                let meta = path.metadata().context("metadata")?;
                if meta.modified().expect("modified") > entry.last_changed {
                    None
                } else {
                    Some(entry)
                }
            }
            None => None,
        })
    }
}

/// Data loader, to load data from files
pub struct DataLoader<F> {
    pending: LoaderTaskSet,
    dir: PathBuf,
    cache: Arc<Cache>,
    _f: std::marker::PhantomData<F>,
}

impl<F: DataFormatHandler> DataLoader<F> {
    /// Create a new data loader from a directory
    pub fn new(dir: &Path) -> anyhow::Result<Self> {
        anyhow::ensure!(dir.is_dir(), "Dir is not a directory: {:?}", dir);
        let cache = Cache::load(&dir.join(".cache"))?;

        Ok(Self {
            pending: LoaderTaskSet::default(),
            cache: Arc::new(cache),
            dir: dir.to_owned(),
            _f: std::marker::PhantomData,
        })
    }

    /// Spawn a task to load data from a file
    fn spawn<Out: Send + 'static>(
        &mut self,
        path: &Path,
        f: impl FnOnce(&Path) -> anyhow::Result<Out> + Send + 'static,
    ) -> DataReceiver<Out> {
        let path = self.dir.join(path);

        let (tx, rx) = oneshot::channel();
        let path = path.to_owned();
        self.pending.spawn_task(move || {
            let out = f(&path).with_context(|| format!("load {path:?}"))?;
            let _ = tx.send(out);
            Ok(())
        });

        DataReceiver(rx)
    }

    /// Load Data from a file
    pub fn load_file<T: Data>(&mut self, path: impl AsRef<Path>) -> DataReceiver<T> {
        self.spawn(path.as_ref(), |path| F::load_from_file::<T>(path))
    }

    /// Load and map data from a file
    pub fn load_map<M: DataMapper>(
        &mut self,
        path: impl AsRef<Path>,
        mapper: M,
    ) -> DataReceiver<M::Out> {
        self.spawn::<M::Out>(path.as_ref(), move |path| {
            let data = F::load_from_file::<M::In>(path)?;
            let out = mapper.map(data)?;
            Ok(out)
        })
    }

    /// Load and map data from a file, but cache the result
    /// so the mapper is only invoked if the file has changed
    pub fn load_map_cached<M: DataMapper>(
        &mut self,
        path: impl AsRef<Path>,
        mapper: M,
    ) -> DataReceiver<M::Out>
    where
        M::Out: Serialize + Data,
    {
        let cache = self.cache.clone();
        self.spawn::<M::Out>(path.as_ref(), move |path| {
            cache.get_or_update(path, |path| {
                let data = F::load_from_file::<M::In>(path)?;
                let out = mapper.map(data)?;
                Ok(out)
            })
        })
    }

    /// Loads data from a given path, with a custom Out type
    pub fn load<Out: Send + 'static>(
        &mut self,
        path: &Path,
        f: impl FnOnce(&Path) -> anyhow::Result<Out> + Send + 'static,
    ) -> DataReceiver<Out> {
        self.spawn(path, f)
    }

    /// Spawn all tasks from a given iterator of paths
    fn spawn_all<Out: Send + 'static, P: AsRef<Path>>(
        &mut self,
        paths: impl Iterator<Item = P>,
        f: impl Fn(&Path) -> anyhow::Result<Out> + Clone + Send + 'static,
    ) -> DataReceiver<Vec<Out>> {
        let mut tasks: FuturesUnordered<_> = paths.map(|path| self.spawn(path.as_ref(), f.clone())).collect();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let mut res = Vec::new();
            while let Some(data) = tasks.next().await {
                res.push(data);
            }
            let _ = tx.send(res);
        });

        DataReceiver(rx)
    }

    /// Load all files from a given iterator of paths
    pub fn load_all_files<T: Data, P: AsRef<Path>>(
        &mut self,
        paths: impl Iterator<Item = P>,
    ) -> DataReceiver<Vec<T>> {
        self.spawn_all(paths, |path| F::load_from_file::<T>(path))
    }


    /// Load all files from a given iterator of paths, with a custom mapper
    pub fn load_all_mapped<M: DataMapper + Clone, P: AsRef<Path>>(
        &mut self,
        paths: impl Iterator<Item = P>,
        mapper: M,
    ) -> DataReceiver<Vec<M::Out>> {
        self.spawn_all(paths, move |path| {
            let data = F::load_from_file::<M::In>(path)?;
            let out = mapper.clone().map(data)?;
            Ok(out)
        })
    }

    /// Load all files from a given iterator of paths, with a custom Out type
    pub fn load_all<Out: Send + 'static, P: AsRef<Path>>(
        &mut self,
        paths: impl Iterator<Item = P>,
        f: impl Fn(&Path) -> anyhow::Result<Out> + Clone + Send + 'static,
    ) -> DataReceiver<Vec<Out>> {
        self.spawn_all(paths, f)
    }

    /// Wait for all pending tasks to finish and update the manifest
    pub async fn wait_all(self) -> anyhow::Result<()> {
        self.pending.wait_all().await?;
        self.cache.save()?;
        Ok(())
    }
}

pub type AutoDataLoader = DataLoader<AutoDataFormatHandler>;

#[cfg(test)]
mod tests {
    use std::{sync::atomic::AtomicBool, time::Duration};

    use super::*;

    #[tokio::test]
    async fn data_loader() {
        let path = Path::new("test_1");
        let _ = std::fs::create_dir(&path);

        let a_file = path.join("a.json");
        let is_mapped = Arc::new(AtomicBool::new(false));

        const HELLO_WORLD: &str = "Hello, World...";
        const HELLO_UNIVERSE: &str = "Hello, Universe...";

        let seq = [
            (HELLO_WORLD, true),
            (HELLO_WORLD, false),
            (HELLO_UNIVERSE, true),
            (HELLO_UNIVERSE, false),
            (HELLO_WORLD, true),
            (HELLO_UNIVERSE, true),
        ];

        for (inp, map) in seq {
            // first
            let json = format!("\"{inp}\"");
            let content = std::fs::read_to_string(&a_file).unwrap();
            if content != json {
                std::fs::write(&a_file, json).unwrap();
            }

            is_mapped.store(false, std::sync::atomic::Ordering::Relaxed);
            let mut loader = AutoDataLoader::new(&path).unwrap();
            let txt = loader.load_file::<String>("a.json");
            let mapped = loader.load_map("a.json", data_mapper_fn(|s: String| Ok(s.len())));
            let is_mapped_ = is_mapped.clone();
            let mapped_cached = loader.load_map_cached(
                "a.json",
                data_mapper_fn(move |s: String| {
                    is_mapped_.store(true, std::sync::atomic::Ordering::Relaxed);
                    Ok(s.len())
                }),
            );
            loader.wait_all().await.unwrap();

            assert_eq!(map, is_mapped.load(std::sync::atomic::Ordering::Relaxed));
            assert_eq!(txt.get(), inp);
            assert_eq!(mapped.get(), inp.len());
            assert_eq!(mapped_cached.get(), inp.len());
            std::thread::sleep(Duration::from_millis(1));
        }


        let mut loader = AutoDataLoader::new(&path).unwrap();
        let all = loader.load_all_files::<String, _>(std::iter::repeat_n(Path::new("a.json"), 10));
        loader.wait_all().await.unwrap();

        let all = all.get();
        assert_eq!(all.len(), 10);
        for s in all {
            assert_eq!(s, HELLO_UNIVERSE);
        }
    }

}

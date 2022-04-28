use crate::direct_io::file::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Options {
    max_resident: usize,
    max_non_resident: usize,
    page_len_scale: usize,
    thread_num: usize,
}

impl Options {
    pub fn max_resident(&mut self, v: usize) -> &mut Self {
        self.max_resident = v;
        self
    }

    pub fn max_non_resident(&mut self, v: usize) -> &mut Self {
        self.max_non_resident = v;
        self
    }

    pub fn page_len_scale(&mut self, v: usize) -> &mut Self {
        self.page_len_scale = v;
        self
    }
    pub fn thread_num(&mut self, n: usize) -> &mut Self {
        self.thread_num = n;
        self
    }
    pub fn get_thread_num(&self) -> usize {
        self.thread_num
    }
}

impl Default for Options {
    fn default() -> Self {
        Self { max_resident: 1024, max_non_resident: 1024, page_len_scale: 1, thread_num: 1 }
    }
}

#[derive(Clone)]
pub struct FileSystem {
    cache: CacheHandle,
    scope_map: Arc<ScopeMap>,
}

assert_impl_all!(FileSystem: Send, Sync);

impl FileSystem {
    pub fn new(options: &Options) -> Self {
        let os_page_len = page_size::get();
        Self { cache:
                   cache::new(cache::Options { max_resident: options.max_resident,
                                               max_non_resident: options.max_non_resident,
                                               page_len:
                                                   os_page_len.checked_mul(options.page_len_scale)
                                                              .unwrap(),
                                               page_align: os_page_len }),
               scope_map: Default::default() }
    }

    pub fn max_resident(&self) -> usize {
        self.cache.max_resident()
    }

    pub fn max_non_resident(&self) -> usize {
        self.cache.max_non_resident()
    }

    pub fn max_page_len(&self) -> usize {
        self.cache.page_len()
    }

    pub fn hit_count(&self) -> u64 {
        self.cache.hit_count()
    }

    pub fn miss_count(&self) -> u64 {
        self.cache.miss_count()
    }

    pub fn read_count(&self) -> u64 {
        self.cache.read_count()
    }

    pub fn write_count(&self) -> u64 {
        self.cache.write_count()
    }

    pub fn open_with(&self, path: impl AsRef<Path>, options: &OpenOptions) -> Result<File> {
        // TODO on Linux can use stat(path) to get the file id and avoid open/close calls for
        // already cached files.
        let file = open(path, options)?;
        let (id, len) = FileId::of(&file)?;

        if let Some(scope) = self.scope_map.get(&id) {
            if let Some(scope) = scope.value().upgrade() {
                return Ok(File::new(scope));
            }
        }

        let scope = FileScope::new(&self.cache, &self.scope_map, file, id, len)?;
        Ok(File::new(scope))
    }

    pub fn open(&self, path: impl AsRef<Path>) -> Result<File> {
        self.open_with(path, OpenOptions::new().read(true).write(true))
    }

    pub fn create(&self, path: impl AsRef<Path>) -> Result<File> {
        self.open_with(path, OpenOptions::new().read(true).write(true).create(true).truncate(true))
    }

    pub fn discard(&self) {
        for scope in self.all_scopes() {
            if let Some(scope) = scope.upgrade() {
                scope.discard();
            }
        }
    }

    pub fn sync_all(&self, sync: FileSync) -> Result<()> {
        for scope in self.all_scopes() {
            if let Some(scope) = scope.upgrade() {
                scope::sync_all(&scope, sync)?;
            }
        }
        Ok(())
    }

    pub fn sync_data(&self, sync: FileSync) -> Result<()> {
        for scope in self.all_scopes() {
            if let Some(scope) = scope.upgrade() {
                scope::sync_data(&scope, sync)?;
            }
        }
        Ok(())
    }

    fn all_scopes(&self) -> Vec<WeakScopeHandle> {
        self.scope_map.iter().map(|e| e.value().clone()).collect::<Vec<_>>()
    }
}

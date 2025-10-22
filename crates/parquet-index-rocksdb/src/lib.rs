/// RocksDB implementation of IndexStorage and FileRegistry
///
/// This crate provides production-ready storage backends for the parquet
/// indexing system using RocksDB.
use anyhow::Result;
use parquet_index::{FileRegistry, IndexStorage, WriteBatch, WriteOp};
use parquet_index_schema::FileId;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

/// RocksDB-backed index storage
///
/// Manages multiple column families for different index types.
/// Thread-safe and supports atomic batch writes.
pub struct RocksDbIndexStorage {
    db: Arc<DB>,
}

impl RocksDbIndexStorage {
    /// Open or create a RocksDB database for index storage
    ///
    /// Creates column families for all index types if they don't exist.
    pub fn open<P: AsRef<Path>>(path: P, column_families: Vec<String>) -> Result<Self> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // Create column family descriptors
        let mut cfs = vec![ColumnFamilyDescriptor::new("default", Options::default())];
        for cf_name in &column_families {
            cfs.push(ColumnFamilyDescriptor::new(cf_name, Options::default()));
        }

        let db = DB::open_cf_descriptors(&db_opts, path.as_ref(), cfs)?;

        info!(
            path = ?path.as_ref(),
            column_families = column_families.len(),
            "Opened RocksDB index storage"
        );

        Ok(Self { db: Arc::new(db) })
    }
}

impl IndexStorage for RocksDbIndexStorage {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| anyhow::anyhow!("Column family not found: {}", cf))?;

        Ok(self.db.get_cf(&cf_handle, key)?)
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| anyhow::anyhow!("Column family not found: {}", cf))?;

        self.db.put_cf(&cf_handle, key, value)?;
        Ok(())
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let mut rocksdb_batch = rocksdb::WriteBatch::default();

        for op in batch.operations {
            match op {
                WriteOp::Put { cf, key, value } => {
                    let cf_handle = self
                        .db
                        .cf_handle(&cf)
                        .ok_or_else(|| anyhow::anyhow!("Column family not found: {}", cf))?;
                    rocksdb_batch.put_cf(&cf_handle, key, value);
                }
                WriteOp::Delete { cf, key } => {
                    let cf_handle = self
                        .db
                        .cf_handle(&cf)
                        .ok_or_else(|| anyhow::anyhow!("Column family not found: {}", cf))?;
                    rocksdb_batch.delete_cf(&cf_handle, key);
                }
            }
        }

        self.db.write(rocksdb_batch)?;
        Ok(())
    }

    fn prefix_iterator<'a>(
        &'a self,
        cf: &str,
        prefix: &[u8],
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a> {
        let cf_handle = match self.db.cf_handle(cf) {
            Some(handle) => handle,
            None => {
                // Return empty iterator if CF doesn't exist
                return Box::new(std::iter::empty());
            }
        };

        let prefix = prefix.to_vec();
        let iter = self
            .db
            .prefix_iterator_cf(&cf_handle, &prefix)
            .map(|result| {
                let (key, value) = result.unwrap();
                (key.to_vec(), value.to_vec())
            })
            .take_while(move |(key, _)| key.starts_with(&prefix));

        Box::new(iter.collect::<Vec<_>>().into_iter())
    }
}

/// RocksDB-backed file registry
///
/// Maintains bidirectional mapping between FileId and file paths.
/// Thread-safe with atomic ID generation.
pub struct RocksDbFileRegistry {
    db: Arc<DB>,
    /// In-memory cache for fast lookups (FileId -> PathBuf)
    path_cache: Mutex<HashMap<FileId, PathBuf>>,
    /// In-memory cache for fast lookups (PathBuf -> FileId)
    id_cache: Mutex<HashMap<PathBuf, FileId>>,
    /// Next FileId to assign
    next_id: Mutex<u32>,
}

impl RocksDbFileRegistry {
    /// Open or create a RocksDB database for file registry
    ///
    /// Uses two column families:
    /// - "file_id_to_path": FileId (u32) -> PathBuf (string)
    /// - "path_to_file_id": PathBuf (string) -> FileId (u32)
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new("default", Options::default()),
            ColumnFamilyDescriptor::new("file_id_to_path", Options::default()),
            ColumnFamilyDescriptor::new("path_to_file_id", Options::default()),
            ColumnFamilyDescriptor::new("metadata", Options::default()),
        ];

        let db = DB::open_cf_descriptors(&db_opts, path.as_ref(), cfs)?;

        // Load existing mappings into cache
        let mut path_cache = HashMap::new();
        let mut id_cache = HashMap::new();
        let mut max_id = 0u32;

        let cf_handle = db.cf_handle("file_id_to_path").unwrap();
        for item in db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start) {
            let (key_bytes, value_bytes) = item?;

            // Deserialize FileId from 4 bytes
            if key_bytes.len() == 4 {
                let file_id =
                    u32::from_be_bytes([key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3]]);
                let path = PathBuf::from(String::from_utf8(value_bytes.to_vec())?);

                path_cache.insert(FileId(file_id), path.clone());
                id_cache.insert(path, FileId(file_id));

                if file_id > max_id {
                    max_id = file_id;
                }
            }
        }

        let next_id = max_id + 1;

        info!(
            path = ?path.as_ref(),
            cached_files = path_cache.len(),
            next_id = next_id,
            "Opened RocksDB file registry"
        );

        Ok(Self {
            db: Arc::new(db),
            path_cache: Mutex::new(path_cache),
            id_cache: Mutex::new(id_cache),
            next_id: Mutex::new(next_id),
        })
    }
}

impl FileRegistry for RocksDbFileRegistry {
    fn register_file(&self, path: &Path) -> Result<FileId> {
        // Check cache first
        {
            let id_cache = self.id_cache.lock().unwrap();
            if let Some(file_id) = id_cache.get(path) {
                debug!(path = ?path, file_id = file_id.0, "File already registered (cache hit)");
                return Ok(*file_id);
            }
        }

        // Check database
        let path_to_id_cf = self
            .db
            .cf_handle("path_to_file_id")
            .ok_or_else(|| anyhow::anyhow!("path_to_file_id CF not found"))?;

        let path_str = path.to_string_lossy();
        if let Some(id_bytes) = self.db.get_cf(&path_to_id_cf, path_str.as_bytes())? {
            if id_bytes.len() == 4 {
                let file_id = FileId(u32::from_be_bytes([
                    id_bytes[0],
                    id_bytes[1],
                    id_bytes[2],
                    id_bytes[3],
                ]));

                // Update caches
                let mut path_cache = self.path_cache.lock().unwrap();
                let mut id_cache = self.id_cache.lock().unwrap();
                path_cache.insert(file_id, path.to_path_buf());
                id_cache.insert(path.to_path_buf(), file_id);

                debug!(path = ?path, file_id = file_id.0, "File already registered (db hit)");
                return Ok(file_id);
            }
        }

        // Assign new FileId
        let file_id = {
            let mut next_id = self.next_id.lock().unwrap();
            let id = FileId(*next_id);
            *next_id += 1;
            id
        };

        // Write to database
        let id_to_path_cf = self
            .db
            .cf_handle("file_id_to_path")
            .ok_or_else(|| anyhow::anyhow!("file_id_to_path CF not found"))?;

        self.db
            .put_cf(&id_to_path_cf, file_id.0.to_be_bytes(), path_str.as_bytes())?;
        self.db
            .put_cf(&path_to_id_cf, path_str.as_bytes(), file_id.0.to_be_bytes())?;

        // Update caches
        {
            let mut path_cache = self.path_cache.lock().unwrap();
            let mut id_cache = self.id_cache.lock().unwrap();
            path_cache.insert(file_id, path.to_path_buf());
            id_cache.insert(path.to_path_buf(), file_id);
        }

        info!(path = ?path, file_id = file_id.0, "Registered new file");
        Ok(file_id)
    }

    fn get_file_path(&self, file_id: FileId) -> Result<PathBuf> {
        // Check cache first
        {
            let path_cache = self.path_cache.lock().unwrap();
            if let Some(path) = path_cache.get(&file_id) {
                return Ok(path.clone());
            }
        }

        // Check database
        let id_to_path_cf = self
            .db
            .cf_handle("file_id_to_path")
            .ok_or_else(|| anyhow::anyhow!("file_id_to_path CF not found"))?;

        if let Some(path_bytes) = self.db.get_cf(&id_to_path_cf, file_id.0.to_be_bytes())? {
            let path = PathBuf::from(String::from_utf8(path_bytes.to_vec())?);

            // Update cache
            let mut path_cache = self.path_cache.lock().unwrap();
            path_cache.insert(file_id, path.clone());

            Ok(path)
        } else {
            Err(anyhow::anyhow!("File not found: {:?}", file_id))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_rocksdb_storage() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_storage");

        // Create storage with column families
        let storage = RocksDbIndexStorage::open(
            &db_path,
            vec!["test_cf1".to_string(), "test_cf2".to_string()],
        )?;

        // Test put/get
        storage.put("test_cf1", b"key1", b"value1")?;
        let value = storage.get("test_cf1", b"key1")?;
        assert_eq!(value, Some(b"value1".to_vec()));

        // Test write batch
        let mut batch = WriteBatch::new();
        batch.put("test_cf2", b"key2".to_vec(), b"value2".to_vec());
        batch.put("test_cf2", b"key3".to_vec(), b"value3".to_vec());
        storage.write_batch(batch)?;

        assert_eq!(storage.get("test_cf2", b"key2")?, Some(b"value2".to_vec()));
        assert_eq!(storage.get("test_cf2", b"key3")?, Some(b"value3".to_vec()));

        Ok(())
    }

    #[test]
    fn test_file_registry() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_registry");

        let registry = RocksDbFileRegistry::open(&db_path)?;

        // Register files
        let path1 = PathBuf::from("/tmp/file1.parquet");
        let path2 = PathBuf::from("/tmp/file2.parquet");

        let id1 = registry.register_file(&path1)?;
        let id2 = registry.register_file(&path2)?;

        assert_ne!(id1, id2);

        // Re-registering should return same ID
        let id1_again = registry.register_file(&path1)?;
        assert_eq!(id1, id1_again);

        // Lookup paths
        assert_eq!(registry.get_file_path(id1)?, path1);
        assert_eq!(registry.get_file_path(id2)?, path2);

        Ok(())
    }

    #[test]
    fn test_file_registry_persistence() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_registry_persist");

        let path1 = PathBuf::from("/tmp/file1.parquet");

        // Register file and close
        let id1 = {
            let registry = RocksDbFileRegistry::open(&db_path)?;
            registry.register_file(&path1)?
        };

        // Reopen and verify
        let registry = RocksDbFileRegistry::open(&db_path)?;
        let id1_reloaded = registry.register_file(&path1)?;

        assert_eq!(id1, id1_reloaded);
        assert_eq!(registry.get_file_path(id1)?, path1);

        Ok(())
    }
}

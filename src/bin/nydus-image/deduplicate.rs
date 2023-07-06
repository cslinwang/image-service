// 2023 Nydus Developers.
//
// SPDX-License-Identifier: Apache-2.0

//! Deduplicate for Chunk.

use anyhow::{Context, Result};
use nydus_api::ConfigV2;
use nydus_builder::Tree;
use nydus_rafs::metadata::RafsSuper;
use nydus_storage::device::BlobInfo;
use rusqlite::{params, Connection};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub trait Database {
    /// Creates a new chunk in the database.
    fn create_chunk(&self) -> Result<(), ()>;

    /// Creates a new blob in the database.
    fn create_blob(&self) -> Result<(), ()>;

    /// Inserts chunk information into the database.
    fn insert_chunk(&self, chunk_info: &ChunkTable) -> Result<(), ()>;

    /// Inserts blob information into the database.
    fn insert_blob(&self, blob_info: &BlobTable) -> Result<(), ()>;

    /// Retrieves all chunk information from the database.
    fn get_chunks(&self) -> Result<Vec<ChunkTable>, ()>;

    /// Retrieves all blob information from the database.
    fn get_blobs(&self) -> Result<Vec<BlobTable>, ()>;
}

pub struct SqliteDatabase {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteDatabase {
    pub fn new(database_path: &str) -> Result<Self, rusqlite::Error> {
        // Delete the database file if it exists.
        if let Ok(metadata) = fs::metadata(database_path) {
            if metadata.is_file() {
                if let Err(err) = fs::remove_file(database_path) {
                    warn!(
                        "Warning: Unable to delete existing database file: {:?}.",
                        err
                    );
                }
            }
        }

        // Attempt to open a new SQLite connection.
        let conn = Connection::open(database_path)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

impl Database for SqliteDatabase {
    fn create_chunk(&self) -> Result<(), ()> {
        let conn = self.conn.lock().unwrap();
        ChunkTable::create(&conn).map_err(|_| ())
    }

    fn create_blob(&self) -> Result<(), ()> {
        let conn = self.conn.lock().unwrap();
        BlobTable::create(&conn).map_err(|_| ())
    }

    fn insert_chunk(&self, chunk: &ChunkTable) -> Result<(), ()> {
        let conn = self.conn.lock().unwrap();
        ChunkTable::insert(&conn, chunk).map_err(|_| ())
    }

    fn insert_blob(&self, blob: &BlobTable) -> Result<(), ()> {
        let conn = self.conn.lock().unwrap();
        BlobTable::insert(&conn, blob).map_err(|_| ())
    }

    fn get_chunks(&self) -> Result<Vec<ChunkTable>, ()> {
        let conn = self.conn.lock().unwrap();
        ChunkTable::list(&conn).map_err(|_| ())
    }

    fn get_blobs(&self) -> Result<Vec<BlobTable>, ()> {
        let conn = self.conn.lock().unwrap();
        BlobTable::list(&conn).map_err(|_| ())
    }
}

pub struct Deduplicate<D: Database + Send + Sync> {
    sb: RafsSuper,
    db: D,
}

impl Deduplicate<SqliteDatabase> {
    pub fn new(
        bootstrap_path: &Path,
        config: Arc<ConfigV2>,
        db_path: &str,
    ) -> anyhow::Result<Self> {
        let (sb, _) = RafsSuper::load_from_file(bootstrap_path, config, false)?;
        let db = SqliteDatabase::new(db_path.strip_prefix('/').unwrap_or(db_path))?;
        Ok(Self { sb, db })
    }

    /// Save metadata to the database: chunk and blob info.
    pub fn save_metadata(&mut self, _mode: Option<&str>) -> anyhow::Result<Vec<Arc<BlobInfo>>> {
        let tree = Tree::from_bootstrap(&self.sb, &mut ())
            .context("Failed to load bootstrap for deduplication.")?;

        // Create the blob table and chunk table.
        self.db
            .create_chunk()
            .map_err(|e| anyhow!("Failed to create chunk: {:?}.", e))?;
        self.db
            .create_blob()
            .map_err(|e| anyhow!("Failed to create blob: {:?}.", e))?;

        // Save blob info to the blob table.
        let blob_infos = self.sb.superblock.get_blob_infos();
        for blob in &blob_infos {
            self.db
                .insert_blob(&BlobTable {
                    blob_id: blob.blob_id().to_string(),
                    blob_compressed_size: blob.compressed_size(),
                    blob_uncompressed_size: blob.uncompressed_size(),
                })
                .map_err(|e| anyhow!("Failed to insert blob: {:?}.", e))?;
        }

        // Save chunk info to the chunk table.
        let pre = &mut |t: &Tree| -> anyhow::Result<()> {
            let node = t.lock_node();
            for chunk in &node.chunks {
                let index: u32 = chunk.inner.blob_index();
                // Get the blob ID.
                let chunk_blob_id = blob_infos[index as usize].blob_id();
                // Insert the chunk into the chunk table.
                self.db
                    .insert_chunk(&ChunkTable {
                        chunk_blob_id,
                        chunk_digest: chunk.inner.id().to_string(),
                        chunk_compressed_size: chunk.inner.compressed_size(),
                        chunk_uncompressed_size: chunk.inner.uncompressed_size(),
                        chunk_compressed_offset: chunk.inner.compressed_offset(),
                        chunk_uncompressed_offset: chunk.inner.uncompressed_offset(),
                    })
                    .map_err(|e| anyhow!("Failed to insert chunk: {:?}.", e))?;
            }
            Ok(())
        };
        tree.walk_dfs_pre(pre)?;

        Ok(self.sb.superblock.get_blob_infos())
    }
}

pub trait Table: Sync + Send + Sized + 'static {
    /// clear table.
    fn clear(conn: &Connection) -> Result<(), rusqlite::Error>;

    /// create table.
    fn create(conn: &Connection) -> Result<(), rusqlite::Error>;

    /// insert data.
    fn insert(conn: &Connection, table: &Self) -> Result<(), rusqlite::Error>;

    /// select data.
    fn list(conn: &Connection) -> Result<Vec<Self>, rusqlite::Error>;
}

#[derive(Debug)]
pub struct ChunkTable {
    chunk_blob_id: String,
    chunk_digest: String,
    chunk_compressed_size: u32,
    chunk_uncompressed_size: u32,
    chunk_compressed_offset: u64,
    chunk_uncompressed_offset: u64,
}

impl Table for ChunkTable {
    fn clear(conn: &Connection) -> Result<(), rusqlite::Error> {
        let _ = conn.execute("DROP TABLE chunk", [])?;
        Ok(())
    }

    fn create(conn: &Connection) -> Result<(), rusqlite::Error> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS chunk (
                id               INTEGER PRIMARY KEY ,
                chunk_blob_id    TEXT NOT NULL,
                chunk_digest     TEXT,
                chunk_compressed_size  INT,
                chunk_uncompressed_size  INT,
                chunk_compressed_offset  INT,
                chunk_uncompressed_offset  INT
            )",
            [],
        )?;
        Ok(())
    }

    fn insert(conn: &Connection, chunk_table: &ChunkTable) -> Result<(), rusqlite::Error> {
        conn.execute(
            "INSERT INTO chunk(chunk_blob_id,chunk_digest,chunk_compressed_size,
                chunk_uncompressed_size,chunk_compressed_offset,chunk_uncompressed_offset)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6);
            ",
            params![
                chunk_table.chunk_blob_id,
                chunk_table.chunk_digest,
                chunk_table.chunk_compressed_size,
                chunk_table.chunk_uncompressed_size,
                chunk_table.chunk_compressed_offset,
                chunk_table.chunk_uncompressed_offset
            ],
        )?;

        Ok(())
    }

    fn list(conn: &Connection) -> Result<Vec<ChunkTable>, rusqlite::Error> {
        let mut stmt: rusqlite::Statement<'_> = conn.prepare(
            "SELECT id, chunk_blob_id, chunk_digest,chunk_compressed_size,
        chunk_uncompressed_size, chunk_compressed_offset, chunk_uncompressed_offset from chunk",
        )?;
        let chunk_iterator = stmt.query_map([], |row| {
            Ok(ChunkTable {
                chunk_blob_id: row.get(1)?,
                chunk_digest: row.get(2)?,
                chunk_compressed_size: row.get(3)?,
                chunk_uncompressed_size: row.get(4)?,
                chunk_compressed_offset: row.get(5)?,
                chunk_uncompressed_offset: row.get(6)?,
            })
        })?;
        let mut chunks = Vec::new();
        for chunk in chunk_iterator {
            chunks.push(chunk?);
        }
        Ok(chunks)
    }
}

#[derive(Debug)]
pub struct BlobTable {
    blob_id: String,
    blob_compressed_size: u64,
    blob_uncompressed_size: u64,
}

impl Table for BlobTable {
    fn clear(conn: &Connection) -> Result<(), rusqlite::Error> {
        let _ = conn.execute("DROP TABLE blob", [])?;
        Ok(())
    }

    fn create(conn: &Connection) -> Result<(), rusqlite::Error> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blob (
                id                      INTEGER PRIMARY KEY ,
                blob_id                 TEXT NOT NULL ,
                blob_compressed_size    INT,
                blob_uncompressed_size  INT
            )",
            [],
        )?;
        Ok(())
    }

    fn insert(conn: &Connection, blob_table: &BlobTable) -> Result<(), rusqlite::Error> {
        conn.execute(
            "INSERT INTO blob (blob_id, blob_compressed_size, blob_uncompressed_size)
            SELECT ?1 , ?2, ?3
            WHERE NOT EXISTS (
                SELECT blob_id
                FROM blob
                WHERE blob_id = ?1
            ) limit 1
            ;
            ",
            params![
                blob_table.blob_id,
                blob_table.blob_compressed_size,
                blob_table.blob_uncompressed_size
            ],
        )?;

        Ok(())
    }

    fn list(conn: &Connection) -> Result<Vec<BlobTable>, rusqlite::Error> {
        let mut stmt =
            conn.prepare("SELECT blob_id, blob_compressed_size, blob_uncompressed_size from blob")?;
        let blob_iterator = stmt.query_map([], |row| {
            Ok(BlobTable {
                blob_id: row.get(0)?,
                blob_compressed_size: row.get(1)?,
                blob_uncompressed_size: row.get(2)?,
            })
        })?;
        let mut blobs = Vec::new();
        for blob in blob_iterator {
            blobs.push(blob?);
        }
        Ok(blobs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{Connection, Result};

    // Setting up an in-memory SQLite database for testing.
    fn setup_db() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;

        BlobTable::create(&conn)?;
        ChunkTable::create(&conn)?;

        Ok(conn)
    }

    #[test]
    fn test_blob_table() -> Result<(), rusqlite::Error> {
        let conn = setup_db()?;

        let blob = BlobTable {
            blob_id: "BLOB123".to_string(),
            blob_compressed_size: "1024".parse::<u64>().unwrap(),
            blob_uncompressed_size: "2048".parse::<u64>().unwrap(),
        };

        BlobTable::insert(&conn, &blob)?;

        let blobs = BlobTable::list(&conn)?;
        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].blob_id, blob.blob_id);
        assert_eq!(blobs[0].blob_compressed_size, blob.blob_compressed_size);
        assert_eq!(blobs[0].blob_uncompressed_size, blob.blob_uncompressed_size);

        Ok(())
    }

    #[test]
    fn test_chunk_table() -> Result<(), rusqlite::Error> {
        let conn = setup_db()?;

        let chunk = ChunkTable {
            chunk_blob_id: "BLOB123".to_string(),
            chunk_digest: "DIGEST123".to_string(),
            chunk_compressed_size: 512,
            chunk_uncompressed_size: 1024,
            chunk_compressed_offset: 0,
            chunk_uncompressed_offset: 0,
        };

        ChunkTable::insert(&conn, &chunk)?;

        let chunks = ChunkTable::list(&conn)?;
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_blob_id, chunk.chunk_blob_id);
        assert_eq!(chunks[0].chunk_digest, chunk.chunk_digest);
        assert_eq!(chunks[0].chunk_compressed_size, chunk.chunk_compressed_size);
        assert_eq!(
            chunks[0].chunk_uncompressed_size,
            chunk.chunk_uncompressed_size
        );
        assert_eq!(
            chunks[0].chunk_compressed_offset,
            chunk.chunk_compressed_offset
        );
        assert_eq!(
            chunks[0].chunk_uncompressed_offset,
            chunk.chunk_uncompressed_offset
        );

        Ok(())
    }
}

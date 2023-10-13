// Copyright (C) 2022 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

//! Generate Chunkdict RAFS bootstrap.
use super::core::node::{ChunkSource, NodeInfo};
use super::{BlobManager, Bootstrap, BootstrapManager, BuildContext, BuildOutput, Tree};
use crate::core::node::Node;
use crate::NodeChunk;
use anyhow::Result;
use nydus_rafs::metadata::chunk::ChunkWrapper;
use nydus_rafs::metadata::inode::InodeWrapper;
use nydus_rafs::metadata::layout::RafsXAttrs;
use nydus_rafs::metadata::RafsVersion;
use nydus_utils::digest::{Algorithm, RafsDigest};
use nydus_utils::lazy_drop;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChunkdictChunkInfo {
    pub image_name: String,
    pub version_name: String,
    pub chunk_blob_id: String,
    pub chunk_digest: String,
    pub chunk_compressed_size: u32,
    pub chunk_uncompressed_size: u32,
    pub chunk_compressed_offset: u64,
    pub chunk_uncompressed_offset: u64,
}

impl ChunkdictChunkInfo {
    pub fn new(
        image_name: String,
        version_name: String,
        chunk_blob_id: String,
        chunk_digest: String,
        chunk_compressed_size: u32,
        chunk_uncompressed_size: u32,
        chunk_compressed_offset: u64,
        chunk_uncompressed_offset: u64,
    ) -> Self {
        Self {
            image_name,
            version_name,
            chunk_blob_id,
            chunk_digest,
            chunk_compressed_size,
            chunk_uncompressed_size,
            chunk_compressed_offset,
            chunk_uncompressed_offset,
        }
    }
}

/// Struct to Generater chunkdict RAFS bootstrap.
pub struct Generater {}

impl Generater {
    // Generate chunkdict RAFS bootstrap.
    #[allow(clippy::too_many_arguments)]
    pub fn generate(
        ctx: &mut BuildContext,
        bootstrap_mgr: &mut BootstrapManager,
        blob_mgr: &mut BlobManager,
        chunkdict: Vec<ChunkdictChunkInfo>,
    ) -> Result<BuildOutput> {
        // build root tree
        let mut tree = Self::build_root_tree()?;

        // build child tree
        let child = Self::build_child_tree(ctx, blob_mgr, &chunkdict)?;
        let mut result = Vec::new();
        result.push(child);
        result.sort_unstable_by(|a, b| a.name().cmp(b.name()));
        tree.children = result;

        // blob
        if let Some((_, blob_ctx)) = blob_mgr.get_current_blob() {
            blob_ctx.set_blob_prefetch_size(ctx);
        }

        // build bootstrap
        let mut bootstrap_ctx = bootstrap_mgr.create_ctx()?;
        let mut bootstrap = Bootstrap::new(tree)?;
        bootstrap.build(ctx, &mut bootstrap_ctx)?;

        // Make sure blob id is updated according to chunkdict.
        if let Some((_, blob_ctx)) = blob_mgr.get_current_blob() {
            if blob_ctx.blob_id.is_empty() {
                blob_ctx.blob_id = chunkdict[0].chunk_blob_id.clone();
            }
        }

        let blob_table = blob_mgr.to_blob_table(ctx)?;
        let storage = &mut bootstrap_mgr.bootstrap_storage;
        bootstrap.dump(ctx, storage, &mut bootstrap_ctx, &blob_table)?;

        lazy_drop(bootstrap_ctx);

        BuildOutput::new(blob_mgr, &bootstrap_mgr.bootstrap_storage)
    }

    /// build root tree
    pub fn build_root_tree() -> Result<Tree> {
        // inode
        let mut inode = InodeWrapper::new(RafsVersion::V6);
        inode.set_ino(0);
        inode.set_uid(1000);
        inode.set_gid(1000);
        inode.set_projid(0);
        inode.set_mode(16893);
        inode.set_nlink(1);
        inode.set_name_size("/".len());
        inode.set_rdev(0);
        let node_info = NodeInfo {
            explicit_uidgid: true,
            src_dev: 66305,
            src_ino: 24772610,
            rdev: 0,
            source: PathBuf::from("/"),
            path: PathBuf::from("/"),
            target: PathBuf::from("/"),
            target_vec: vec![OsString::from("/")],
            symlink: None,
            xattrs: RafsXAttrs::default(),
            v6_force_extended_inode: true,
        };
        let root_node = Node::new(inode, node_info, 0);
        let tree = Tree::new(root_node);
        return Ok(tree);
    }

    /// build child tree
    fn build_child_tree(
        ctx: &mut BuildContext,
        blob_mgr: &mut BlobManager,
        chunkdict: &[ChunkdictChunkInfo],
    ) -> Result<Tree> {
        // node
        let mut inode = InodeWrapper::new(RafsVersion::V6);
        inode.set_ino(0);
        inode.set_uid(1000);
        inode.set_gid(1000);
        inode.set_projid(0);
        inode.set_mode(33204);
        inode.set_nlink(1);
        inode.set_name_size("chunkdict".len());
        inode.set_rdev(0);
        let node_info = NodeInfo {
            explicit_uidgid: true,
            src_dev: 66305,
            src_ino: 24775126,
            rdev: 0,
            source: PathBuf::from("/"),
            path: PathBuf::from("/chunkdict"),
            target: PathBuf::from("/chunkdict"),
            target_vec: vec![OsString::from("/"), OsString::from("/chunkdict")],
            symlink: None,
            xattrs: RafsXAttrs::new(),
            v6_force_extended_inode: true,
        };
        let mut node = Node::new(inode, node_info, 0);

        // insert chunks
        Self::insert_chunks(ctx, blob_mgr, &mut node, chunkdict)?;

        let child = Tree::new(node);
        child
            .lock_node()
            .v5_set_dir_size(ctx.fs_version, &child.children);
        Ok(child)
    }

    /// insert chunks
    fn insert_chunks(
        ctx: &mut BuildContext,
        blob_mgr: &mut BlobManager,
        node: &mut Node,
        chunkdict: &[ChunkdictChunkInfo],
    ) -> Result<()> {
        for chunk_info in chunkdict.iter() {
            let chunk_size: u32 = chunk_info.chunk_compressed_size;
            let file_offset = 1 as u64 * chunk_size as u64;
            ctx.fs_version = RafsVersion::V6;
            let mut chunk = ChunkWrapper::new(RafsVersion::V6);
            // chunk_wrapper.set_index(chunk.clone());
            let (blob_index, blob_ctx) = blob_mgr.get_or_create_current_blob(ctx)?;
            let chunk_index = blob_ctx.alloc_chunk_index()?;
            chunk.set_blob_index(blob_index);
            chunk.set_index(chunk_index);
            chunk.set_file_offset(file_offset);
            chunk.set_compressed_size(chunk_info.chunk_compressed_size);
            chunk.set_compressed_offset(chunk_info.chunk_compressed_offset);
            chunk.set_uncompressed_size(chunk_info.chunk_uncompressed_size);
            chunk.set_uncompressed_offset(chunk_info.chunk_uncompressed_offset);
            chunk.set_id(RafsDigest::from_buf(
                chunk_info.chunk_digest.as_bytes(),
                Algorithm::Sha256,
            ));

            let chunk = Arc::new(chunk);
            // blob_ctx.add_chunk_meta_info(&chunk, chunk_info)?;
            node.chunks.push(NodeChunk {
                source: ChunkSource::Build,
                inner: chunk,
            });
        }
        Ok(())
    }
}

/*
    Copyright 2019 Supercomputing Systems AG

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

*/

use std::fs::{self, File};
use std::io::stdin;
use std::io::Write;
use std::path::Path;
use std::slice;
use std::str;
use std::sync::{
    mpsc::{channel, Sender},
    Mutex,
};
use std::collections::HashMap;
use my_node_runtime::{
    substratee_registry::ShardIdentifier, Event, Hash, Header, SignedBlock, UncheckedExtrinsic,
};

use sgx_types::*;
use codec::{Decode, Encode};
use log::*;
use sp_core::{
    crypto::{AccountId32, Ss58Codec},
    sr25519,
    storage::StorageKey,
    Pair,
    H256,
};

use substratee_worker_primitives::block::{
    SignedBlock as SignedSidechainBlock,
    SidechainBlockNumber
};

use rocksdb::{DB, WriteBatch};


/// sidechain database path
const SIDECHAIN_DB_PATH: &str = "../bin/sidechainblock_db";
/// key value of sidechain db of last block
const LAST_BLOCK_KEY: &[u8] = b"last_sidechainblock";
/// key value of the stored shards vector
const STORED_SHARDS_KEY: &[u8] = b"stored_shards";

/// DB errors.
#[derive(Debug)]
pub enum DBError {
    /// Unexpected empty key-value pair
    UnexpectedEmpty(Vec<u8>),
    /// RocksDB Error
    OperationalError(String),
    /// Decoding Error
    DecodeError,
}

/// Contains the blocknumber and blokhash of the
/// last sidechain block
#[derive(PartialEq, Eq, Clone, Encode, Decode, Debug, Default)]
pub struct LastSidechainBlock {
    /// hash of the last sidechain block
    hash: H256,
    /// block number of the last sidechain block
    number: SidechainBlockNumber,
}

pub struct SidechainDB {
    ///  newly produced sidechain blocks
    pub signed_blocks: Vec<SignedSidechainBlock>,
    /// map to last sidechain block of every shard
    pub last_sidechain_blocks: HashMap<ShardIdentifier, LastSidechainBlock>,
}

impl SidechainDB {
    pub fn new(signed_blocks: Vec<SignedSidechainBlock>) -> SidechainDB {
        SidechainDB {signed_blocks, last_sidechain_blocks: HashMap::new()}
    }

    pub fn new_from_encoded(mut encoded_signed_blocks: &[u8]) -> Result<SidechainDB, DBError> {
        let signed_blocks: Vec<SignedSidechainBlock> = match Decode::decode(&mut encoded_signed_blocks) {
            Ok(blocks) => blocks,
            Err(e) => {
                error!("Could not decode confirmation calls: {:?}", e);
                return Err(DBError::DecodeError)
            }
        };
        Ok(SidechainDB::new(signed_blocks))
    }

    /// update sidechain storage
    pub fn update_db(&self) -> Result<(), DBError> {
        // Store sidechain blocks
        if !self.signed_blocks.is_empty() {
            let mut batch = WriteBatch::default();
            let db = DB::open_default("../bin/sidechainblock_db").unwrap();
            //FIXME: Proper error handling !
            let mut currently_stored_shards: Vec<ShardIdentifier> = match db.get(STORED_SHARDS_KEY) {
                Ok(Some(shards)) => {
                    Decode::decode(&mut shards.as_slice()).unwrap()
                },
                Ok(None) => {
                    println!("value not found");
                    vec![]
                },
                Err(e) => {
                    println!("operational problem encountered: {}", e);
                    vec![]
                },
            };
            let mut new_shard = false;
            for signed_block in self.signed_blocks.clone().into_iter() {
                // Block hash -> Signed Block
                let block_hash = signed_block.hash();
                batch.put(&block_hash, &signed_block.encode().as_slice());
                // (Shard, Block number) -> Blockhash (for block pruning)
                let block_shard = signed_block.block().shard_id();
                let block_nr = signed_block.block().block_number();
                batch.put(&(block_shard, block_nr).encode().as_slice(), &block_hash);
                // (last_block_key, shard) -> (Blockhash, BlockNr) current blockchain state
                let current_last_block = LastSidechainBlock{
                    hash: block_hash.into(),
                    number: block_nr,
                };
                self.last_sidechain_blocks.insert(block_shard, current_last_block);
                // stored_shards_key -> vec<shard>
                if !currently_stored_shards.contains(&block_shard) {
                    currently_stored_shards.push(block_shard);
                    new_shard = true;
                }
            }
            if new_shard {
                batch.put(STORED_SHARDS_KEY, currently_stored_shards.encode());
            }
            if let Err(e) = db.write(batch) {
                error!("Could not write batch to sidechain db due to {}", e);
            };
            // update last blocks
            for (shard, last_block) in self.last_sidechain_blocks.iter() {
                if let Err(e) = db.put((LAST_BLOCK_KEY, shard).encode(), last_block.encode()) {
                    error!("Could not write last block to sidechain db due to {}", e);
                };
            }
            match db.get(STORED_SHARDS_KEY) {
                Ok(Some(encoded_shards)) => {
                    let shards: Vec<ShardIdentifier> = Decode::decode(&mut encoded_shards.as_slice()).unwrap();
                    println!("retrieved shards {:?}", shards);
                    for shard in shards {
                        match db.get((LAST_BLOCK_KEY, shard).encode()) {
                            Ok(Some(encoded_last_block)) => {
                                let last_block = LastSidechainBlock::decode(&mut encoded_last_block.as_slice()).unwrap();
                                println!("retrieved value {:?}", last_block);
                            },
                            Ok(None) => println!("value not found"),
                            Err(e) => println!("operational problem encountered: {}", e),
                        }
                    }
                },
                Ok(None) => println!("value not found"),
                Err(e) => println!("operational problem encountered: {}", e),
            }
        }
        Ok(())
    }
}
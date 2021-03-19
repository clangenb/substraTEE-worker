// This file is part of Substrate.

// Copyright (C) 2018-2020 Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: GPL-3.0-or-later WITH Classpath-exception-2.0

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

//! Chain api required for the operation pool.
extern crate alloc;
use alloc::{boxed::Box, vec::Vec};
use log::*;

use codec::Encode;
use jsonrpc_core::futures::future::{ready, Future, Ready};
use std::{marker::PhantomData, pin::Pin};

use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, Hash as HashT, Header as HeaderT},
    transaction_validity::{
        TransactionValidity, TransactionValidityError, UnknownTransaction, ValidTransaction,
        TransactionTag as Tag,
    },
};

use crate::top_pool::pool::{ChainApi, ExtrinsicHash, NumberFor};
use crate::top_pool::primitives::TrustedOperationSource;

use substratee_stf::{Getter, TrustedOperation as StfTrustedOperation, AccountId};
use substratee_worker_primitives::BlockHash as SidechainBlockHash;

use crate::rpc::error;

/// The operation pool logic for full client.
pub struct FillerChainApi<Block> {
    _marker: PhantomData<Block>,
}

impl<Block> FillerChainApi<Block> {
    /// Create new operation pool logic.
    pub fn new() -> Self {
        FillerChainApi {
            _marker: Default::default(),
        }
    }
}

fn to_tag(nonce: u64, from: AccountId) -> Tag {
	let mut data = [0u8; 40];
	data[..8].copy_from_slice(&nonce.to_le_bytes()[..]);
	data[8..].copy_from_slice(&from.as_ref());
	data.to_vec()
}

impl<Block> ChainApi for FillerChainApi<Block>
where
    Block: BlockT,
{
    type Block = Block;
    type Error = error::Error;
    type ValidationFuture =
        Pin<Box<dyn Future<Output = error::Result<TransactionValidity>> + Send>>;
    type BodyFuture = Ready<error::Result<Option<Vec<StfTrustedOperation>>>>;

    fn block_body(&self, _id: &BlockId<Self::Block>) -> Self::BodyFuture {
        ready(Ok(None))
    }

    fn validate_transaction(
        &self,
        _at: &BlockId<Self::Block>,
        _source: TrustedOperationSource,
        uxt: StfTrustedOperation,
    ) -> Self::ValidationFuture {
        let operation = match uxt {
            StfTrustedOperation::direct_call(signed_call) => {
                let nonce = signed_call.nonce;
                let from = signed_call.call.account();
                let require_vec = if nonce > 1 {
					vec![to_tag(nonce-1, from.clone())]
				} else {
                    vec![]
                };

                ValidTransaction {
                    priority: 1 << 20,
                    requires: require_vec,
                    provides: vec![to_tag(nonce, from.clone())],
                    longevity: 3,
                    propagate: true,
                }
            },
            StfTrustedOperation::get(getter) => match getter {
                Getter::public(_) => {
                    return Box::pin(ready(Ok(Err(TransactionValidityError::Unknown(
                        UnknownTransaction::CannotLookup,
                    )))))
                }
                Getter::trusted(trusted_getter) => ValidTransaction {
                    priority: 1 << 20,
                    requires: vec![],
                    provides: vec![trusted_getter.signature.encode()],
                    longevity: 3,
                    propagate: true,
                },
            },
            _ => {
                return Box::pin(ready(Ok(Err(TransactionValidityError::Unknown(
                    UnknownTransaction::CannotLookup,
                )))))
            }
        };
        Box::pin(ready(Ok(Ok(operation))))
    }

    fn block_id_to_number(
        &self,
        at: &BlockId<Self::Block>,
    ) -> error::Result<Option<NumberFor<Self>>> {
        Ok(match at {
            BlockId::Number(num) => Some(*num),
            BlockId::Hash(_) => None,
        })
    }

    fn block_id_to_hash(
        &self,
        at: &BlockId<Self::Block>,
    ) -> error::Result<Option<SidechainBlockHash>> {
        Ok(match at {
            //BlockId::Hash(x) => Some(x.clone()),
            BlockId::Hash(_x) => None,
            // dummy
            BlockId::Number(_num) => None,
        })
    }

    fn hash_and_length(&self, ex: &StfTrustedOperation) -> (ExtrinsicHash<Self>, usize) {
        debug!("[Pool] creating hash of {:?}", ex);
        ex.using_encoded(|x| {
            (
                <<Block::Header as HeaderT>::Hashing as HashT>::hash(x),
                x.len(),
            )
        })
    }
}

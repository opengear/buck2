/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::hash::Hash;
use std::sync::Arc;

use allocative::Allocative;
use async_trait::async_trait;
use derive_more::Display;
use dice::DetectCycles;
use dice::Dice;
use dice::DiceComputations;
use dice::DiceKeyDyn;
use dice::InjectedKey;
use dice::Key;
use dice::NoValueSerialize;
use dice::UserComputationData;
use dice::ValueSerialize;
use dice_futures::cancellation::CancellationContext;
use dupe::Dupe;
use pagable::Pagable;
use pagable::pagable_typetag;

/// Stands in for the objects a command attaches to its transaction — event dispatcher,
/// materializer, RE connection manager. Identity is what matters, so the payload is an `Arc`
/// whose pointer can be compared against the one that was injected.
struct Marker(Arc<u32>);

#[derive(Allocative, Clone, Debug, Display, Eq, PartialEq, Hash, Pagable)]
#[display("{:?}", self)]
#[pagable_typetag(DiceKeyDyn)]
struct Leaf;

#[async_trait]
impl InjectedKey for Leaf {
    type Value = u32;

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        NoValueSerialize::<Self::Value>::new()
    }

    fn equality(x: &Self::Value, y: &Self::Value) -> bool {
        x == y
    }
}

#[derive(Allocative, Clone, Debug, Display, Eq, PartialEq, Hash, Pagable)]
#[display("{:?}", self)]
#[pagable_typetag(DiceKeyDyn)]
struct Derived;

#[async_trait]
impl Key for Derived {
    type Value = u32;

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        NoValueSerialize::<Self::Value>::new()
    }

    async fn compute(
        &self,
        ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        ctx.compute(&Leaf).await.unwrap() * 10
    }

    fn equality(x: &Self::Value, y: &Self::Value) -> bool {
        x == y
    }
}

fn dice_holding(marker: &Arc<u32>) -> (Arc<Dice>, UserComputationData) {
    let dice = Dice::builder().build(DetectCycles::Enabled);
    let mut user_data = UserComputationData::new();
    user_data.data.set(Marker(marker.dupe()));
    (dice, user_data)
}

/// An updater taken from a transaction commits into a transaction that still holds the same
/// user data, rather than the default set a fresh `Dice::updater` would install.
#[tokio::test]
async fn a_transaction_updater_keeps_the_user_data_it_came_from() {
    let marker = Arc::new(7);
    let (dice, user_data) = dice_holding(&marker);

    let first = {
        let mut updater = dice.updater_with_data(user_data);
        updater.changed_to(vec![(Leaf, 1)]).unwrap();
        updater.commit().await
    };

    let second = first.dupe().into_updater().commit().await;

    let carried = second
        .per_transaction_data()
        .data
        .get::<Marker>()
        .expect("user data reaches the committed transaction");
    assert!(
        Arc::ptr_eq(&carried.0, &marker),
        "the committed transaction holds the injected objects themselves, not copies"
    );
}

/// Keys invalidated through an updater taken from a transaction recompute in the transaction
/// that updater commits, which is what lets a command re-run an action it has already run.
#[tokio::test]
async fn invalidating_through_a_transaction_updater_recomputes_dependents() {
    let marker = Arc::new(7);
    let (dice, user_data) = dice_holding(&marker);

    let first = {
        let mut updater = dice.updater_with_data(user_data);
        updater.changed_to(vec![(Leaf, 1)]).unwrap();
        updater.commit().await
    };
    assert_eq!(*first.compute(&Derived).await.unwrap(), 10);

    let second = {
        let mut updater = first.dupe().into_updater();
        updater.changed_to(vec![(Leaf, 2)]).unwrap();
        updater.commit().await
    };

    assert_eq!(*second.compute(&Derived).await.unwrap(), 20);
    // The transaction the updater came from is pinned to its own version, so it keeps serving
    // the value it computed. A round of repair has to commit to see anything new.
    assert_eq!(*first.compute(&Derived).await.unwrap(), 10);
}

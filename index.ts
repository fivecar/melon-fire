import { Database, DirtyRaw } from "@nozbe/watermelondb";
import {
  SyncDatabaseChangeSet,
  SyncPullArgs,
  SyncPullResult,
  SyncPushArgs,
  synchronize,
} from "@nozbe/watermelondb/sync";
import firestore, {
  FirebaseFirestoreTypes,
} from "@react-native-firebase/firestore";

interface MelonFireRoot {
  melonLatestRevision?: number;
  melonLatestDate?: string; // ISO
}

type MelonBatchTokens = { [revision: string]: string };

interface MelonFireBaseDoc extends MelonFireRoot {
  // For each revision that was a big batch, its firestore docId is mapped in
  // this object. You can then find the batchDoc at baseDoc/melonBatches/[token]
  melonBatchTokens: MelonBatchTokens;
}

interface MelonBatchDoc extends MelonFireRoot {
  // We've made a compromise here by storing deleted IDs from a batch all within
  // its main BatchDoc. Firestore's 1MB limit means that you can hit an issue if
  // you store, say, a few tens of thousands of deletes within one sync. If
  // you'd like to overcome that limit, you'd instead have to store each delete
  // in its own document under BatchDoc, which seems over the top. Some of this
  // is about how much storage and $ you're willing to pay on a regular basis.
  deletes: TableDeletes;
}

// You need to delete these before uploading any records. And because we made
// a mistake and let these through in batched writes in the past, we now also
// need to clean these out of past records when pulling changes.
interface WatermelonClientInternals {
  _status?: string; // Need to delete this from the raw record
  _changed?: string; // same - need to del.
}

interface ChangeRecord {
  id: string;
  melonFireChange?: never; // Shipped in v1, so must delete from loaded rows
  melonFireRevision: number;
  _status: never; // Guarantees we don't let internals through
  _changed: never;
}

type ChangeRecordWithWatermelonInternals = ChangeRecord &
  WatermelonClientInternals;

interface DeleteRef {
  ref: FirebaseFirestoreTypes.DocumentReference;
  id: string;
}

interface AllDeleteRefs {
  [tableName: string]: DeleteRef[];
}

// We store each revision's deletes as its own document. This makes it much less
// likely to run into Firestore's 1MB limit.
interface DeleteRecord {
  revision: number;
  deletes: TableDeletes;
}

interface TableDeletes {
  [tableName: string]: string[]; // ids of records to delete
}

// We use this when loading changes so that sequential records on the same id
// (e.g. an early create followed by a late update) are merged into one record.
// Note we only ever have updates (and no creates) because we use
// sendCreatedAsUpdated: true in the sync config.
interface ChangeLoadMap {
  [tableName: string]: {
    updated: { [id: string]: DirtyRaw };
    deleted: { [id: string]: true };
  };
}

const MAX_TRANSACTION_WRITES = 500; // From firebase docs
const BATCH_COLLECTION = "melonBatches";
const DELETE_COLLECTION = "melonDeletes"; // Contains DeleteRecord docs
const MIN_REVISION = 1;

export async function syncMelonFire(
  database: Database,
  baseDoc: FirebaseFirestoreTypes.DocumentReference,
) {
  // WatermelonDB docs say to attempt sync twice, because a failure the first
  // time can be resolved by re-pulling and reattempting again.
  try {
    await sync(database, baseDoc);
  } catch (err) {
    // Deliberately not catch errors on this one so that caller can know.
    await sync(database, baseDoc);
  }
}

async function sync(
  database: Database,
  baseDoc: FirebaseFirestoreTypes.DocumentReference,
) {
  return await synchronize({
    database,
    pullChanges: async params =>
      await pullChanges(Object.keys(database.schema.tables), baseDoc, params),
    pushChanges: async params => await pushChanges(baseDoc, params),
    // The docs worringly say that "some edge cases may not be handled as well."
    // I don't even know what that really means, but we need this flag to be
    // true because we only keep the most recent record/version of each record.
    sendCreatedAsUpdated: true,
  });
}

/*
Pull is more complicated that it sounds because of batching limits in Firestore.
Tables of records (rows in the DB == docs in our backup) are kept as collections
under baseDoc when possible (i.e. when firestore batches allow atomic commits),
but larger changes are atomically added as subDocs in the baseDoc/melonBatches
collection.

This means, whenever pulling a set of revisions, we need to apply each revision
in order, pulling them from baseDoc or melonBatches as appropriate.

A note on "timestamps": we don't use them, so that we avoid tricky sync issues
where timestamps might be different on different machines (though I'm told large
scale cloud systems like Firestore might guarantee that timestamps are the same
across all machines). Instead, we use a revision number that's incremented each
time you write. So when you pull, the returned timestamp will be one revision
beyond the last written revision (i.e. it's exclusive); when you push, you'll
push at the same timestamp as when you last pulled.
*/
async function pullChanges(
  tables: string[],
  baseDoc: FirebaseFirestoreTypes.DocumentReference,
  params: SyncPullArgs,
): Promise<SyncPullResult> {
  const { lastPulledAt, schemaVersion, migration } = params;
  const startRevision = lastPulledAt === null ? MIN_REVISION : lastPulledAt;
  const baseSnap = await baseDoc.get();
  const [endRevision, batchTokens] = revisionFromBaseSnap(baseSnap);
  // All ids added to changeMap are url-decoded
  const changeMap: ChangeLoadMap = {};

  tables.forEach(table => {
    changeMap[table] = {
      updated: {},
      deleted: {},
    };
  });

  let start = startRevision;
  let end = start;

  // It's much faster to pull forward changes over multiple revisions when
  // they're contiguous in baseDoc -- so we go through revisions here in baseDoc
  // clumps until they're interrupted by a melonBatch. Note that you can't just
  // pull _all_ baseDoc revs first and then sprinkle in the melonBatches,
  // because you can't ignore their ordering (e.g. a melonBatch might create a
  // row that a later baseDoc revision relies on).
  while (start < endRevision) {
    while (!batchTokens?.hasOwnProperty(end.toString()) && end < endRevision) {
      end++;
    }
    if (end === start) {
      const token = batchTokens[end.toString()];
      const root = baseDoc.collection(BATCH_COLLECTION).doc(token);

      end++; // End is exclusive, so you need to bump it before merging below
      const [rootSnap] = await Promise.all([
        root.get(),
        mergeCreatesAndUpdates(root, start, end, tables, changeMap),
      ]);

      // Now add deletions from this batch
      const rootDoc = rootSnap.data() as MelonBatchDoc;
      Object.keys(rootDoc.deletes).forEach(table => {
        rootDoc.deletes[table].forEach(
          id => (changeMap[table].deleted[decodeURIComponent(id)] = true),
        );
      });
    } else {
      await mergeCreatesAndUpdates(baseDoc, start, end, tables, changeMap);

      // Now add deletions from all relevant revisions.
      const snaps = await baseDoc
        .collection(DELETE_COLLECTION)
        .where("revision", ">=", start)
        .where("revision", "<", end)
        .get();
      const records = snaps.docs.map(doc => doc.data() as DeleteRecord);

      for (const record of records) {
        Object.keys(record.deletes).forEach(table => {
          record.deletes[table].forEach(
            id => (changeMap[table].deleted[decodeURIComponent(id)] = true),
          );
        });
      }
    }
    start = end;
  }

  const changes: SyncDatabaseChangeSet = {};

  tables.forEach(table => {
    changes[table] = {
      created: [],
      updated: Object.values(changeMap[table].updated).filter(
        row => !(row.id in changeMap[table].deleted),
      ),
      deleted: Object.keys(changeMap[table].deleted),
    };
  });

  return {
    changes,
    timestamp: endRevision,
  };
}

// Adds to changeMap allchanges from a root doc (whether that's baseDoc or a
// batchDoc). All returned ids are already url-decoded.
async function mergeCreatesAndUpdates(
  root: FirebaseFirestoreTypes.DocumentReference,
  startRevision: number,
  endRevision: number, // exclusive!
  tables: string[],
  changeMap: ChangeLoadMap,
) {
  await Promise.all(
    tables.map(async table => {
      const refs = await root
        .collection(table)
        .where("melonFireRevision", ">=", startRevision)
        .where("melonFireRevision", "<", endRevision)
        .orderBy("melonFireRevision")
        .get();

      refs.docs.forEach(doc => {
        // We shouldn't have uploaded those internals... but now we have to
        // accept that our type, coming out of the cloud, will always have the
        // possibility of having internals.
        const storedData = doc.data() as ChangeRecordWithWatermelonInternals;
        const rec = removeMelonFields(removeWatermelonInternals(storedData));

        changeMap[table].updated[decodeURIComponent(rec.id)] = rec;
      });
    }),
  );
}

function removeWatermelonInternals(
  obj: ChangeRecordWithWatermelonInternals,
): ChangeRecord {
  delete obj._status;
  delete obj._changed;
  return obj;
}

// Prior to fixing a bug, we had pushed up batch records with Watermelon
// internals. So we now need to strip them out here because some folks still
// have those fields in their cloud records.
function removeMelonFields(record: ChangeRecord): ChangeRecord {
  delete record.melonFireChange;
  delete record.melonFireRevision;
  return record;
}

/*
The trick here is atomicity. You want to make sure the backup moves forward one
entire quanta at a time -- you don't want just some changes with the new
timestamp: you want either all changes from a timestamp, or none of them.

But the problem is that Firestore transactions can at most have 500 writes. If
you have more than 500 changes that need to be atomic, you'll need something
else to guarantee that. We prefer pushAllChanges when things fit into one
transaction, since its saves space; but we pushBatchedChanges otherwise, to
preserve atomicity.
*/
async function pushChanges(
  baseDoc: FirebaseFirestoreTypes.DocumentReference,
  params: SyncPushArgs,
) {
  const delRefs = await findDeleteRefs(baseDoc, params);
  let delChanges = 0;

  Object.keys(delRefs).forEach(table => (delChanges += delRefs[table].length));

  if (countChanges(params.changes, delChanges) < MAX_TRANSACTION_WRITES) {
    return await pushAllChanges(baseDoc, params, delRefs);
  } else {
    return await pushBatchedChanges(baseDoc, params, delRefs);
  }
}

/**
 * returns refs with url-encoded ids!
 */
async function findDeleteRefs(
  baseDoc: FirebaseFirestoreTypes.DocumentReference,
  params: SyncPushArgs,
): Promise<AllDeleteRefs> {
  const delRefs: AllDeleteRefs = {};

  for (const table of Object.keys(params.changes)) {
    const deletedIds = params.changes[table].deleted;
    const baseSnaps = await Promise.all(
      deletedIds.map(id =>
        baseDoc.collection(table).doc(encodeURIComponent(id)).get(),
      ),
    );
    const baseExists = baseSnaps.filter(snap => snap.exists);

    if (baseExists.length > 0) {
      delRefs[table] = baseExists.map(snap => ({
        ref: snap.ref,
        id: encodeURIComponent(snap.data().id),
      }));
    }

    // We need to search through all past batches, even though we know
    // params.lastPulledAt, because the record could have been created from
    // anytime back. This isn't awesome.
    const batchSnaps = await baseDoc.collection(BATCH_COLLECTION).get();

    // Next, check for deleted rows in all relevant batches. Remember that the
    // same row can occur in multiple batches (e.g. multiple updates).
    const delInBatchPromises: Promise<
      FirebaseFirestoreTypes.DocumentSnapshot<FirebaseFirestoreTypes.DocumentData>
    >[] = [];
    deletedIds.forEach(id =>
      delInBatchPromises.push(
        ...batchSnaps.docs.map(batch =>
          batch.ref.collection(table).doc(encodeURIComponent(id)).get(),
        ),
      ),
    );
    const delInBatchSnaps = await Promise.all(delInBatchPromises);
    const delInBatchExists = delInBatchSnaps.filter(snap => snap.exists);

    if (delInBatchExists.length > 0) {
      const refs = delInBatchExists.map(snap => ({
        ref: snap.ref,
        id: encodeURIComponent(snap.data().id),
      }));
      if (table in delRefs) {
        delRefs[table].push(...refs);
      } else {
        delRefs[table] = refs;
      }
    }
  }

  return delRefs;
}

function revisionFromBaseSnap(
  snap: FirebaseFirestoreTypes.DocumentSnapshot<FirebaseFirestoreTypes.DocumentData>,
): [number, MelonBatchTokens | undefined] {
  const existingDoc = snap.data() as MelonFireBaseDoc | undefined;
  const revision = existingDoc?.melonLatestRevision
    ? existingDoc?.melonLatestRevision + 1
    : MIN_REVISION;

  return [revision, existingDoc?.melonBatchTokens];
}

/*
Writes every set/update as a doc, augmenting it with a change marker and
a revision number so that we can pull them efficiently later.
Requires that params.changes contains less than MAX_TRANSACTION_WRITES!
 */
async function pushAllChanges(
  baseDoc: FirebaseFirestoreTypes.DocumentReference,
  params: SyncPushArgs,
  delRefs: AllDeleteRefs,
): Promise<void> {
  const { lastPulledAt, changes } = params;

  return await firestore().runTransaction(async trans => {
    const baseSnap = await trans.get(baseDoc);
    const [revision] = revisionFromBaseSnap(baseSnap);
    const tableDeletes: TableDeletes = {};

    if (revision !== lastPulledAt) {
      throw Error(
        `Local DB out of sync. Last pulled changes up to ${
          lastPulledAt - 1
        }, but now attempting to push revision ${revision}`,
      );
    }

    Object.keys(changes).forEach(table => {
      function storeRawInTrans(raw: DirtyRaw) {
        const rawRec: ChangeRecordWithWatermelonInternals = {
          ...(raw as ChangeRecordWithWatermelonInternals),
          melonFireRevision: revision,
        };
        const rec = removeWatermelonInternals(rawRec);

        // This is a set, not an update, for reasons outlined in
        // pushBatchedChanges. TL;DR is that you can't be guaranteed any
        // particular row/doc exists; they might be sequestered in a tokened
        // batch.
        trans.set(
          baseDoc.collection(table).doc(encodeURIComponent(rec.id)),
          rec,
        );
      }

      changes[table].created.forEach(storeRawInTrans);
      changes[table].updated.forEach(storeRawInTrans);

      if (table in delRefs) {
        // delRef ids are already url-encoded
        tableDeletes[table] = delRefs[table].map(ref => ref.id);
        delRefs[table].forEach(delRef => {
          trans.delete(delRef.ref);
        });
      }
    });

    if (Object.keys(tableDeletes).length) {
      const record: DeleteRecord = {
        revision,
        deletes: tableDeletes,
      };
      trans.set(baseDoc.collection(DELETE_COLLECTION).doc(), record);
    }

    const updatedBase: Omit<MelonFireBaseDoc, "melonBatchTokens"> = {
      melonLatestRevision: revision,
      melonLatestDate: new Date().toISOString(),
    };

    // This is why you need less than MAX_TRANSACTION_WRITES of changes: you
    // need this one more write in order to update the baseDoc. Merging to not
    // overwrite any batch tokens.
    trans.set(baseDoc, updatedBase, { merge: true });
  });
}

/*
When we have more changes than can fit in a transaction, we can't just start
bulk-overwriting existing docs/rows in our baseDoc's table collections (because
if we fail to complete 100% of the writes, we'll leave those tables in an
inconsistent state). Instead, we need to write all the docs/rows in a place
where no one else is impacted, and only make an atomic update to
melonLatestRevision once 100% of docs/rows have been written.

To do this, we write all our changes in a fresh doc under baseDoc/melonBatches.
This doc then contains collections for all the tables we need to write. We can
fail without impacting DB integrity because no one knows about this subdoc until
we succeed with 100% of our doc/row writes, at which point we populate the
"tokens" object in baseDoc with our revision number and batch token atomically.

There are two consequences to this approach:

1. Changed rows in the DB are duplicated for every create/update that happens
in a batch larger than 500 writes. Instead of just overwriting exitsing rows,
we need to instead copy them into a melonBatch doc. Without larger transactions,
I do not believe there is an alternative to this (that doesn't involve
wholesale copying of the entire database into atomically-switchable a/b
copies).
2. It complicates pullChanges, which must now merge these batches intelligently.
*/
async function pushBatchedChanges(
  baseDoc: FirebaseFirestoreTypes.DocumentReference,
  params: SyncPushArgs,
  delRefs: AllDeleteRefs,
): Promise<void> {
  const { lastPulledAt, changes } = params;
  const batchDoc = baseDoc.collection(BATCH_COLLECTION).doc();
  const baseSnap = await baseDoc.get();
  const [revision, batchTokens] = revisionFromBaseSnap(baseSnap);

  let batch = new BatchWriter(batchDoc, revision);
  const deletes: TableDeletes = {};

  if (revision !== lastPulledAt) {
    throw Error(
      `Local DB out of sync. Last pulled changes up to ${
        lastPulledAt - 1
      }, but now attempting to push revision ${revision}`,
    );
  }

  // This is deliberately written to serially await through these iterables
  // so that BatchWriter can actually work reliably.
  for (const table of Object.keys(changes)) {
    for (const raw of Object.values(changes[table].created)) {
      await batch.created(
        table,
        removeWatermelonInternals(raw as ChangeRecordWithWatermelonInternals),
      );
    }

    for (const raw of Object.values(changes[table].updated)) {
      await batch.updated(
        table,
        removeWatermelonInternals(raw as ChangeRecordWithWatermelonInternals),
      );
    }

    if (table in delRefs) {
      // Note that delRef ids are already url-encoded
      deletes[table] = delRefs[table].map(ref => ref.id);
      await batch.deleted(delRefs[table].map(dr => dr.ref));
    }
  }

  await batch.flush();

  // If we haven't thrown by now, we're 100% successful writing all rows. Now
  // we attempt an atomic write that will integrate us into the main backup.
  try {
    await firestore().runTransaction(async trans => {
      const date = new Date().toISOString();
      const baseUpdate: Omit<MelonFireBaseDoc, "melonDeletes"> = {
        melonBatchTokens: {
          ...batchTokens,
          [revision]: batchDoc.id,
        },
        melonLatestDate: date,
        melonLatestRevision: revision,
      };
      const root: MelonBatchDoc = {
        melonLatestRevision: revision,
        melonLatestDate: date,
        deletes,
      };

      if (revision !== lastPulledAt) {
        throw Error(
          `Local DB out of sync. Last pulled changes up to ${
            lastPulledAt - 1
          }, but now attempting to push revision ${revision}`,
        );
      }

      trans.set(batchDoc, root);

      // We merge when writing baseDoc so that we don't overwrite other things.
      trans.set(baseDoc, baseUpdate, { merge: true });
    });
  } catch (err) {
    // If we fail to atomically integrate, attempt to roll back our changes.
    const rollbackRefs = [];

    for (const table of Object.keys(changes)) {
      const writtenDocs = await batchDoc.collection(table).get();

      rollbackRefs.push(writtenDocs.docs.map(doc => doc.ref));
    }
    for (
      let iBlock = 0;
      iBlock < rollbackRefs.length / MAX_TRANSACTION_WRITES + 1;
      iBlock++
    ) {
      const block = rollbackRefs.slice(
        iBlock * MAX_TRANSACTION_WRITES,
        (iBlock + 1) * MAX_TRANSACTION_WRITES,
      );
      const batch = firestore().batch();

      for (const ref of block) {
        batch.delete(ref);
      }
      await batch.commit();
    }

    // Now rethrow so that synchronize know we failed, and will reattempt.
    throw err;
  }
}

function countChanges(
  changes: SyncDatabaseChangeSet,
  deleteCount: number,
): number {
  // You can't just assume changes[table].deleted.length is the number of
  // deletes, because it's possible that a row occurs in multiple batches and
  // thus requires multiple deletes. Use the result findDeleteRefs instead.
  const tableCounts = Object.keys(changes).map(table => {
    return changes[table].created.length + changes[table].updated.length;
  });

  // If we have any deletions at all, we need to account for the extra
  // DeleteRecord doc that we'll write. So we +1 when we have any deletions.
  return (
    tableCounts.reduce((prev, cur) => prev + cur, 0) +
    (deleteCount > 0 ? deleteCount + 1 : 0)
  );
}

// Collects writes until the batch count is hit, at which point it commits
// the batch and begins collecting more. Note that deletions are complicated --
// since the existence of the doc can't be guaranteed within the batch itself,
// deletions need to be explicitly tracked and later reconciled during
// pullChanges.
//
// Usage: You create one of these, call a bunch of set/delete, and then flush()
// when you're done. You must flush, because there might be an unwritten partial
// batch.
class BatchWriter {
  private batch: FirebaseFirestoreTypes.WriteBatch;
  private count: number;
  private doc: FirebaseFirestoreTypes.DocumentReference;
  private revision: number;

  constructor(doc: FirebaseFirestoreTypes.DocumentReference, revision: number) {
    this.doc = doc;
    this.batch = firestore().batch();
    this.count = 0;
    this.revision = revision;
  }

  private async flushBatch() {
    await this.batch.commit();
    this.batch = firestore().batch();
    this.count = 0;
  }

  private async bumpCount() {
    this.count++;
    if (this.count === MAX_TRANSACTION_WRITES) {
      return await this.flushBatch();
    }
  }

  public async flush() {
    await this.flushBatch();
    return this;
  }

  public async created(table: string, rec: ChangeRecord) {
    await this.write(table, rec);
    return this;
  }

  public async updated(table: string, rec: ChangeRecord) {
    await this.write(table, rec);
    return this;
  }

  public async deleted(refs: FirebaseFirestoreTypes.DocumentReference[]) {
    const headCount = Math.min(
      refs.length,
      MAX_TRANSACTION_WRITES - this.count,
    );

    // Fill the rest of the batch and flush
    refs.slice(0, headCount).forEach(ref => this.batch.delete(ref));
    this.count += headCount;

    const restRefs = refs.slice(headCount);

    if (this.count === MAX_TRANSACTION_WRITES) {
      await this.flushBatch();

      // Now do whole blocks at a time, including the final partial block
      for (
        let iBlock = 0;
        iBlock < restRefs.length / MAX_TRANSACTION_WRITES + 1;
        iBlock++
      ) {
        const block = restRefs.slice(
          iBlock * MAX_TRANSACTION_WRITES,
          (iBlock + 1) * MAX_TRANSACTION_WRITES,
        );

        for (const ref of block) {
          this.batch.delete(ref);
        }
        this.count += block.length;

        // The final block might be partial, in which case don't flush yet
        if (this.count === MAX_TRANSACTION_WRITES) {
          this.flushBatch();
        }
      }
    }

    return this;
  }

  /**
   * By the time you call this, you should be sure that
   * removeWatermelonInternals has already been called so that the record
   * doesn't contain those internals.
   */
  private async write(table: string, rec: ChangeRecord) {
    const ref = this.doc.collection(table).doc(encodeURIComponent(rec.id));
    const data: ChangeRecord = {
      ...rec,
      melonFireRevision: this.revision,
    };

    // Ok - this needs to be explained a bit. We "set" instead of "update" (even
    // for update changes) because this row is written in a collection where the
    // record might not exist (because we're not overwriting the main table).
    // For instance, a previous pushChanges might have created the row, but this
    // separate pushChanges is updating it. So we always "set", even during an
    // update. This will continue to work as long as WatermelonDB sends complete
    // rows for each update.
    this.batch.set(ref, data);
    await this.bumpCount();

    return this;
  }
}

export const exportsForTesting = {
  pullChanges,
  pushChanges,
  countChanges,
};

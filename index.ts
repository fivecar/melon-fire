import { Database, DirtyRaw } from "@nozbe/watermelondb";
import {
  SyncDatabaseChangeSet,
  synchronize,
  SyncPullArgs,
  SyncPullResult,
  SyncPushArgs,
} from "@nozbe/watermelondb/sync";
import firestore, {
  FirebaseFirestoreTypes,
} from "@react-native-firebase/firestore";

interface MelonFireRoot {
  melonLatestRevision: number;
  melonLatestDate: string; // ISO
  melonDeletes: TableDeletesByRevision;
}

interface MelonFireBaseDoc extends MelonFireRoot {
  // For each revision that was a big batch, its firestore docId is mapped in
  // this object. You can then find the batchDoc at baseDoc/melonBatches/[token]
  melonBatchTokens: { [revision: string]: string };
}

interface ChangeRecord {
  id: string;
  melonFireChange: ChangeType;
  melonFireRevision: number;
  _status?: string; // Need to delete this from the raw record
  _changed?: string; // same - need to del.
}

interface TableDeletes {
  [tableName: string]: string[]; // ids of records to delete
}

type TableDeletesByRevision = { [revision: string]: TableDeletes };

type ChangeType = "created" | "updated" | "deleted";
type ChangeWithId = Partial<ChangeRecord> & Pick<ChangeRecord, "id">;

const MAX_TRANSACTION_WRITES = 500; // From firebase docs
const BATCH_COLLECTION = "melonBatches";
const MIN_REVISION = 1;

export default async function syncMelonFire(
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
    pullChanges: async params => await pullChanges(database, baseDoc, params),
    pushChanges: async params => await pushChanges(baseDoc, params),
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
*/
async function pullChanges(
  db: Database,
  baseDoc: FirebaseFirestoreTypes.DocumentReference,
  params: SyncPullArgs,
): Promise<SyncPullResult> {
  const { lastPulledAt, schemaVersion, migration } = params;
  const startRevision = lastPulledAt === null ? MIN_REVISION : lastPulledAt;
  const baseSnap = await baseDoc.get();
  const existingDoc = baseSnap.data() as MelonFireBaseDoc | undefined;
  // endRevision is _exclusive_
  const endRevision = existingDoc?.melonLatestRevision
    ? existingDoc?.melonLatestRevision + 1
    : startRevision;
  const tables = Object.keys(db.schema.tables);
  const changes: SyncDatabaseChangeSet = {};

  tables.forEach(table => {
    changes[table] = {
      created: [],
      updated: [],
      deleted: [],
    };
  });

  let start = startRevision;
  let end = start;

  // It's must faster to pull forward changes over multiple revisions when
  // they're contiguous in baseDoc -- so we go through revisions here in baseDoc
  // clumps until they're interrupted by a melonBatch. Note that you can't just
  // pull _all_ baseDoc revs first and then sprinkle in the melonBatches,
  // because you can't ignore their ordering (e.g. a melonBatch might create a
  // row that a later baseDoc revision relies on).
  while (start < endRevision) {
    while (
      !existingDoc?.melonBatchTokens?.hasOwnProperty(end.toString()) &&
      end < endRevision
    ) {
      end++;
    }
    if (end === start) {
      const token = existingDoc!.melonBatchTokens[end.toString()];
      const root = baseDoc.collection(BATCH_COLLECTION).doc(token);

      await mergeChanges(root, start, end, tables, changes);
      end++;
    } else {
      await mergeChanges(baseDoc, start, end, tables, changes);
    }
    start = end;
  }

  return {
    changes,
    timestamp: endRevision,
  };
}

// Modifies the contents of the "changes" object to include all changes from
// a root doc (whether that's baseDoc or a batchDoc).
async function mergeChanges(
  root: FirebaseFirestoreTypes.DocumentReference,
  startRevision: number,
  endRevision: number, // exclusive!
  tables: string[],
  changes: SyncDatabaseChangeSet,
) {
  await Promise.all(
    tables.map(async table => {
      const refs = await root
        .collection(table)
        .where("melonFireRevision", ">=", startRevision)
        .where("melonFireRevision", "<", endRevision)
        .orderBy("melonFireRevision")
        .get();
      const recs = refs.docs.map(doc => doc.data() as ChangeRecord);

      changes[table].created.push(
        ...recs
          .filter(rec => rec.melonFireChange === "created")
          .map(removeMelonFields),
      );

      // New updates need to overwrite old updates that might already exist
      const updates = recs
        .filter(rec => rec.melonFireChange === "updated")
        .map(removeMelonFields);
      updates.forEach(update => {
        const index = changes[table].updated.findIndex(
          up => up.id === update.id,
        );

        if (index >= 0) {
          changes[table].updated[index] = update;
        } else {
          changes[table].updated.push(update);
        }
      });
    }),
  );

  // Now add deletions from all revisions.
  const rootSnap = await root.get();
  const rootDoc = rootSnap.data() as MelonFireRoot | undefined;
  if (rootDoc) {
    // rootDoc might not exist, e.g. on first backup's pull
    for (let rev = startRevision; rev < endRevision; rev++) {
      const revStr = rev.toString();

      if (rootDoc.melonDeletes.hasOwnProperty(revStr)) {
        const deletes = rootDoc.melonDeletes[revStr];

        Object.keys(deletes).forEach(table => {
          changes[table].deleted.push(...deletes[table]);
        });
      }
    }
  }
}

function removeMelonFields(record: ChangeRecord): ChangeWithId {
  delete (record as ChangeWithId).melonFireChange;
  delete (record as ChangeWithId).melonFireRevision;
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
  if (countChanges(params.changes) < MAX_TRANSACTION_WRITES) {
    return await pushAllChanges(baseDoc, params);
  } else {
    return await pushBatchedChanges(baseDoc, params);
  }
}

/*
Writes every set/update as a doc, augmenting it with a change marker and
a revision number so that we can pull them efficiently later.
Requires that params.changes contains less than MAX_TRANSACTION_WRITES!
 */
async function pushAllChanges(
  baseDoc: FirebaseFirestoreTypes.DocumentReference,
  params: SyncPushArgs,
): Promise<void> {
  const { lastPulledAt, changes } = params;

  return await firestore().runTransaction(async trans => {
    const baseSnap = await trans.get(baseDoc);
    const existingDoc = baseSnap.data() as MelonFireBaseDoc | undefined;
    const revision = existingDoc?.melonLatestRevision
      ? existingDoc?.melonLatestRevision + 1
      : MIN_REVISION;
    const tableDeletes: TableDeletes = {};

    if (revision !== lastPulledAt) {
      throw Error(
        `Local DB out of sync. Last pulled changes up to ${
          lastPulledAt - 1
        }, but now attempting to push revision ${revision}`,
      );
    }

    Object.keys(params.changes).forEach(table => {
      changes[table].created.forEach(raw => {
        const rec: ChangeRecord = {
          ...(raw as ChangeWithId),
          melonFireChange: "created",
          melonFireRevision: revision,
        };
        delete rec._status;
        delete rec._changed;

        trans.set(baseDoc.collection(table).doc(rec.id), rec);
      });

      changes[table].updated.forEach(raw => {
        const rec: ChangeRecord = {
          ...(raw as ChangeWithId),
          melonFireChange: "updated",
          melonFireRevision: revision,
        };
        delete rec._status;
        delete rec._changed;
        // This is a set, not an update, for reasons outlined in
        // pushBatchedChanges. TL;DR is that you can't be guaranteed any
        // particular row/doc exists; they might be sequestered in a tokened
        // batch.
        trans.set(baseDoc.collection(table).doc(rec.id), rec);
      });

      if (changes[table].deleted.length) {
        tableDeletes[table] = changes[table].deleted;
      }
    });

    const dels: Pick<MelonFireBaseDoc, "melonDeletes"> | {} = Object.keys(
      tableDeletes,
    ).length
      ? {
          melonDeletes: {
            ...existingDoc?.melonDeletes,
            [revision]: tableDeletes,
          },
        }
      : {};
    const updatedBase: Omit<
      MelonFireBaseDoc,
      "melonBatchTokens" | "melonDeletes"
    > = {
      melonLatestRevision: revision,
      melonLatestDate: new Date().toISOString(),
      ...dels,
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
): Promise<void> {
  const { lastPulledAt, changes } = params;
  const batchDoc = baseDoc.collection(BATCH_COLLECTION).doc();
  let batch = new BatchWriter(batchDoc, lastPulledAt);
  const deletes: TableDeletes = {};

  // This is deliberately written to serially await through these iterables
  // so that BatchWriter can actually work reliably.
  for (const table of Object.keys(changes)) {
    for (const raw of Object.values(changes[table].created)) {
      await batch.created(table, raw);
    }

    for (const raw of Object.values(changes[table].updated)) {
      await batch.updated(table, raw);
    }

    deletes[table] = changes[table].deleted;
  }

  await batch.flush();

  // If we haven't thrown by now, we're 100% successful writing all rows. Now
  // we attempt an atomic write that will integrate us into the main backup.
  try {
    await firestore().runTransaction(async trans => {
      const baseSnap = await trans.get(baseDoc);
      const existingDoc = baseSnap.data() as MelonFireBaseDoc | undefined;
      const revision = existingDoc?.melonLatestRevision
        ? existingDoc?.melonLatestRevision + 1
        : MIN_REVISION;
      const date = new Date().toISOString();
      const baseUpdate: Omit<MelonFireBaseDoc, "melonDeletes"> = {
        melonBatchTokens: {
          ...existingDoc?.melonBatchTokens,
          [revision]: batchDoc.id,
        },
        melonLatestDate: date,
        melonLatestRevision: revision,
      };
      const root: MelonFireRoot = {
        melonLatestRevision: revision,
        melonLatestDate: date,
        melonDeletes: { [revision]: deletes },
      };

      if (revision !== lastPulledAt) {
        throw Error(
          `Local DB out of sync. Last pulled changes up to ${
            lastPulledAt - 1
          }, but now attempting to push revision ${revision}`,
        );
      }

      trans.set(batchDoc, root);

      // We merge when writing baseDoc so that we don't overwrite other things,
      // like its "deletes" records.
      trans.set(baseDoc, baseUpdate, { merge: true });
    });
  } catch (err) {
    // If we fail to atomically integrate, attempt to roll back our changes.
    await batch.rollback();

    // Now rethrow so that synchronize know we failed, and will reattempt.
    throw err;
  }
}

function countChanges(changes: SyncDatabaseChangeSet): number {
  // We don't count deletes because those are written separately in the baseDoc
  // or batchDoc.
  const tableCounts = Object.keys(changes).map(table => {
    return changes[table].created.length + changes[table].updated.length;
  });

  return tableCounts.reduce((prev, cur) => prev + cur, 0);
}

// Collects writes until the batch count is hit, at which point it commits
// the batch and begins collecting more. Note that deletions are complicated --
// since the existence of the doc can't be guaranteed within the batch itself,
// deletions need to be explicitly tracked and later reconciled during
// pullChanges.
//
// Usage: You create one of these, call a bunch of set/delete, and then flush()
// when you're done. You must flush, because there might be an unwritten partial
// batch. rollback() will delete everything that was ever written.
class BatchWriter {
  private batch;
  private count;
  private doc;
  private touched: FirebaseFirestoreTypes.DocumentReference[];
  private revision;

  constructor(doc: FirebaseFirestoreTypes.DocumentReference, revision: number) {
    this.doc = doc;
    this.batch = firestore().batch();
    this.count = 0;
    this.touched = [];
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

  public async created(table: string, raw: DirtyRaw) {
    await this.write("created", table, raw);
    return this;
  }

  public async updated(table: string, raw: DirtyRaw) {
    await this.write("updated", table, raw);
    return this;
  }

  private async write(change: ChangeType, table: string, raw: DirtyRaw) {
    const ref = this.doc.collection(table).doc(raw.id);
    const data: ChangeRecord = {
      ...(raw as ChangeWithId),
      melonFireChange: change,
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

    // Only push ref after bumpCount so that we're sure it's been counted or
    // written (so that rollback can rely on that fact).
    this.touched.push(ref);
    return this;
  }

  public async rollback() {
    if (this.count > 0) {
      // We don't try to roll back refs which haven't been batch-commited yet.
      this.touched = this.touched.slice(0, -this.count);

      // This resets our overall state in case people try to use us more.
      this.batch = firestore().batch();
      this.count = 0;
    }

    let i = 0;
    while (i < this.touched.length) {
      const batch = firestore().batch(); // Deleting in batches is faster
      const chunkSize = Math.min(
        MAX_TRANSACTION_WRITES,
        this.touched.length - i,
      );
      const chunk = this.touched.slice(i, i + chunkSize);

      chunk.forEach(ref => batch.delete(ref));
      await batch.commit();

      i += chunkSize;
    }

    this.touched = [];
  }
}

# MelonFire: WatermelonDB / Firestore Sync

MelonFire implements the [WatermelonDB sync
protocol](https://nozbe.github.io/WatermelonDB/Advanced/Sync.html) via
[RNFirebase](https://rnfirebase.io/firestore/usage)'s React Native integration
with Firestore. By integrating MelonFire, you can easily back up your
WatermelonDB database with your Firestore instance in the cloud without writing
any code yourself.

## Installation

```
npm install melon-fire
```

or

```
yarn install melon-fire
```

## Usage

```
import firestore from "@react-native-firebase/firestore";
import { Database } from  "@nozbe/watermelondb";
import syncMelonFire from "melon-fire";

async function onSyncButtonPushOrRandomTimer(
	db: Database,
	userId: string,
) {
	const syncDocRef = firestore()
		.collections("users")
		.doc(userId);
	return await syncMelonFire(db, syncDocRef);
}
```

### Notes on Usage

- `userId` doesn't have to literally be a user id. It can be any valid Firestore
  doc ID that you want associated with the backup/sync in Firestore. Similarly,
  your collection doesn't have to be `"users"` — it can be anything you want.
- You pass a `DocumentReference` to MelonFire, which will then write several of
  its own fields into the document (currently: `melonLatestRevision`,
  `melonLatestDate`, `melonDeletes`, and `melonBatchTokens`). It'll also create
  a collection under `syncDocRef` for each table in your database. If you don't
  mind these fields and collections living in a shared doc with other things
  (e.g. if you want MelonFire's data to be kept in your user's profile doc),
  it's fine; MelonFire won't overwrite your other fields/collections.
- `syncMelonFire` will throw if there are sync errors. Note that, in accordance
  with WatermelonDB's guidance, `syncMelonFire` will actually catch the first
  error it receives and retry sync one time on its own. This, for instance,
  automatically resolves the most common sync issue, which is that another
  writer has updated the cloud records since you last pulled changes. But in
  cases where the first retry fails, `syncMelonFire` will throw.
- Depending on the size of your DB and your network connection, `syncMelonFire`
  could take a while (especially if, say, you're pulling or pushing a ton of
  changes). You don't necessarily have to await `syncMelonFire` if you're sure
  your app can move on and do other things while sync is processing in the
  background, but that might be a bad idea for most apps because you don't want
  to be modifying the database even while sync is running (since your new
  changes might conflict with what it pulls down from the cloud). But this is
  ultimately up to your judgment — you know your app best.
- Please consider MelonFire's [limitations](#limits) before adopting it.

## How It Works

The [WatermelonDB sync
protocol](https://nozbe.github.io/WatermelonDB/Advanced/Sync.html) is actually
non-trivial to implement, especially if you'd like to sync your DB into
Firestore. For example:

1. Timestamps are suggested by the WatermelonDB docs, but they're subtly and
   annoyingly tricky. Have you considered leap seconds?
   Spring-forward/fall-back? Millisecond-level jitter/inconsistency on servers?
   If you get timestamps wrong, your DB backup gets corrupted.
2. Firestore has a [batch write / transaction
   limit](https://firebase.google.com/docs/firestore/manage-data/transactions#:~:text=Each%20transaction%20or%20batch%20of,a%20maximum%20of%20500%20documents.)
   of 500 writes. How will you guarantee atomic write operations when backing
   up, if you have more than 500 edits since the last write? Existing solutions
   ignore this, but MelonFire makes it possible to keep a consistent backup in
   Firestore despite this limitation.
3. WatermelonDB requires you to track the difference between creates and
   updates, as well as to maintain lazy deletion records. This makes the
   "obvious" implementation of simply calling `firestore().blah.set()`,
   `firestore().blah.update()`, and `firestore().blah.delete()` incorrect.
4. Reads can be inconsistent if you're not careful to shield against concurrency
   between readers and writers. MelonFire ensures consistent reads without
   succumbing to Firestore's transaction limits.

### Architecture

MelonFire is a client-side library that relies on RNFirebase's Firestore
integration. This means you won't need to push or maintain cloud functions, but
can instead just integrate MelonFire into your app for easy DB sync or backup
into Firestore. MelonFire overcomes the challenges listed above by:

1. using atomically-incremented change counters instead of timestamps. This
   guarantees ordering even if the server changes its mind about what time it
   is. This also makes sync independent of client time.
2. using Firestore transactions when you have less than 500 changes, for maximum
   efficiency. But when your changeset is larger than that, MelonFire writes
   your changes into a side doc and a set of collections (one per table); these
   are then atomically integrated into the main backup only when all writes are
   successful, guaranteeing that half-completed large batches never corrupt the
   database.
3. tracking changesets distinctly so that creates remain creates, updates remain
   updates, and deletes are persisted lazily in order for future updates to be
   correct regardless of the number of revisions pulled.
4. implementing an append-only, version-tracked set of records in Firestore that
   shield readers against concurrent writers.

Though MelonFire's architecture allows it to maintain consistency despite
Firestore's 500-write limit, note that a consequence is that large batch writes
are stored in Firestore without merging with past or future records. At the
extreme, if your app only ever pushes huge batches (e.g. you modify the same 10k
records every day, and then want to sync with the cloud daily), you'll be
storing a copy of those records for each sync. Whereas if you only ever pushed
less than 500 records at a time, MelonFire would never have more than one doc in
Firestore for any one row in your database. If someone has a clever (and
functionally correct) alternative to this implementation, [please let me
know](mailto:philip@sparkanvil.com).

## <a name="limits"></a>Limitations

- `schemaVersion` and `migrations` from WatermelonDB's sync protocol aren't
  supported yet. This isn't a technical limitation — it's merely because I don't
  myself need these yet. If you have a need to process migrations, please submit
  a PR and I'll be happy to integrate your work. Or I'll get to it when I need
  it. If you use MelonFire in the meantime, just realize this means that
  migrations won't work (i.e. if you start changing your schema, this library
  won't maintain consistent data for you).
- The amount of storage needed in Firestore could exceed your database size,
  depending on your usage pattern. As described in the Architecture section
  above, MelonFire replicates records when you push more than 500 changes at a
  time. If you sync more often than that, your Firestore data consumption will
  stay proportional with your database size.
- MelonFire has only been tested in one app (knowingly, by me) so far. It
  doesn't have automation tests. I'm super-open to PRs that strengthen the
  testing, as well as people [opening
  issues](https://github.com/fivecar/melon-fire/issues) on MelonFire. I just
  wanted to be upfront about the robustness of the library.

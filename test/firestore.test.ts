/**
 * I got this from https://github.com/firebase/quickstart-testing
 */
import {
  apps,
  clearFirestoreData,
  initializeTestApp as mockInitializeTestApp,
} from "@firebase/rules-unit-testing";
import { afterAll, describe, expect, jest, test } from "@jest/globals";
import {
  collection,
  doc,
  getDoc,
  getDocs,
  getFirestore,
} from "@react-native-firebase/firestore";
import { exportsForTesting } from "../index";

// Implements the subset of the @react-native-firebase/firestore v26 modular API
// that melon-fire uses, on top of the namespaced web SDK exposed by
// @firebase/rules-unit-testing (which talks to the Firestore emulator).
jest.mock("@react-native-firebase/firestore", () => {
  const db = mockInitializeTestApp({
    projectId: PROJECT_ID,
    auth: null,
  }).firestore();

  // RNFB snapshots expose exists() as a method; the web v8 SDK uses a property.
  function wrapSnapshot(snap: any): any {
    return new Proxy(snap, {
      get(target, prop) {
        if (prop === "exists") {
          const existsValue = target.exists;
          return () => existsValue;
        }
        const value = target[prop];
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
  }

  // parent is a Firestore, CollectionReference or DocumentReference
  function docFn(parent: any, ...segments: string[]): any {
    if (segments.length === 0) {
      return parent.doc(); // auto-id under a CollectionReference
    }
    if (typeof parent.doc === "function") {
      return parent.doc(segments.join("/"));
    }
    // DocumentReference: no .doc(), so descend through a collection first
    return parent
      .collection(segments[0])
      .doc(segments.slice(1).join("/"));
  }

  return {
    __esModule: true,
    getFirestore: () => db,
    doc: docFn,
    collection: (parent: any, ...segments: string[]) =>
      parent.collection(segments.join("/")),
    query: (q: any, ...constraints: Array<(q: any) => any>) =>
      constraints.reduce((acc, constraint) => constraint(acc), q),
    where:
      (...args: any[]) =>
      (q: any) =>
        q.where(...args),
    orderBy:
      (...args: any[]) =>
      (q: any) =>
        q.orderBy(...args),
    getDoc: async (ref: any) => wrapSnapshot(await ref.get()),
    getDocs: async (q: any) => {
      const snap = await q.get();
      return {
        empty: snap.empty,
        size: snap.size,
        docs: snap.docs.map(wrapSnapshot),
      };
    },
    runTransaction: (firestore: any, fn: (trans: any) => Promise<any>) =>
      firestore.runTransaction(fn),
    writeBatch: (firestore: any) => firestore.batch(),
  };
});
jest.setTimeout(15000);

const { pullChanges, pushChanges, countChanges } = exportsForTesting;

/**
 * The emulator will accept any project ID for testing.
 */
const PROJECT_ID = "melon-fire-test";

beforeEach(async () => {
  // Clear the database between tests
  await clearFirestoreData({ projectId: PROJECT_ID });
});

afterAll(async () => {
  // Delete all the FirebaseApp instances created during testing
  // Note: this does not affect or clear any data
  await Promise.all(apps().map(app => app.delete()));
});

describe("Melon Fire", () => {
  test("performs first pull", async () => {
    const profile = doc(getFirestore(), "backups", "alpha");
    const res = await pullChanges(["entries"], profile, { lastPulledAt: null });

    expect(res.changes.entries).toBeDefined();
    for (const change in res.changes.entries) {
      expect(res.changes.entries[change].length).toBe(0);
    }
    expect(res.timestamp).toBe(1);
  });

  test("pushes on the first time", async () => {
    const profile = doc(getFirestore(), "backups", "bravo");

    await pushChanges(profile, {
      changes: {
        entries: {
          created: [{ id: "aaa", data: "hello" }],
          updated: [],
          deleted: [],
        },
      },
      lastPulledAt: 1,
    });

    const entries = await getDocs(collection(profile, "entries"));

    expect(entries.docs.length).toBe(1);
    expect(entries.docs[0].data().id).toBe("aaa");
    expect(entries.docs[0].data().data).toBe("hello");
  });

  test("pulls first pushed changes", async () => {
    const profile = doc(getFirestore(), "backups", "charlie");

    await pushChanges(profile, {
      changes: {
        entries: {
          created: [{ id: "aaa", data: "hello" }],
          updated: [],
          deleted: [],
        },
      },
      lastPulledAt: 1,
    });
    const res = await pullChanges(["entries"], profile, { lastPulledAt: null });

    // Because we sendCreatesAsUpdates, we expect the created entry to be
    // sent back as an update.
    expect(res.changes.entries.created.length).toBe(0);
    expect(res.changes.entries.updated.length).toBe(1);
    expect(res.changes.entries.updated[0].id).toBe("aaa");
    expect(res.changes.entries.updated[0].data).toBe("hello");
    expect(res.changes.entries.deleted.length).toBe(0);

    expect(res.timestamp).toBe(2);
  });

  test("pulls two pushed changes", async () => {
    const profile = doc(getFirestore(), "backups", "delta");

    await pushChanges(profile, {
      changes: {
        entries: {
          created: [{ id: "aaa", data: "hello" }],
          updated: [],
          deleted: [],
        },
      },
      lastPulledAt: 1,
    });
    await pushChanges(profile, {
      changes: {
        entries: {
          created: [{ id: "bbb", data: "yo" }],
          updated: [{ id: "aaa", data: "it's me" }],
          deleted: [],
        },
      },
      lastPulledAt: 2,
    });

    const res = await pullChanges(["entries"], profile, { lastPulledAt: null });

    expect(res.changes.entries.created.length).toBe(0);
    expect(res.changes.entries.updated.length).toBe(2);
    expect(res.changes.entries.updated[0].id).toBe("aaa");
    expect(res.changes.entries.updated[0].data).toBe("it's me");
    expect(res.changes.entries.updated[1].id).toBe("bbb");
    expect(res.changes.entries.updated[1].data).toBe("yo");
    expect(res.changes.entries.deleted.length).toBe(0);

    expect(res.timestamp).toBe(3);
  });

  test("pulls only requested changes since last pull", async () => {
    const profile = doc(getFirestore(), "backups", "echo");

    await pushChanges(profile, {
      changes: {
        entries: {
          created: [{ id: "aaa", data: "hello" }],
          updated: [],
          deleted: [],
        },
      },
      lastPulledAt: 1,
    });
    await pushChanges(profile, {
      changes: {
        entries: {
          created: [{ id: "bbb", data: "yo" }],
          updated: [{ id: "aaa", data: "it's me" }],
          deleted: [],
        },
      },
      lastPulledAt: 2,
    });

    const res = await pullChanges(["entries"], profile, { lastPulledAt: 3 });

    expect(res.changes.entries.created.length).toBe(0);
    expect(res.changes.entries.updated.length).toBe(0);
    expect(res.changes.entries.deleted.length).toBe(0);
    expect(res.timestamp).toBe(3);
  });

  test("deletes record before sending changes back", async () => {
    const profile = doc(getFirestore(), "backups", "foxtrot");

    await pushChanges(profile, {
      changes: {
        entries: {
          created: [{ id: "aaa", data: "hello" }],
          updated: [],
          deleted: [],
        },
      },
      lastPulledAt: 1,
    });
    await pushChanges(profile, {
      changes: {
        entries: {
          created: [],
          updated: [],
          deleted: ["aaa"],
        },
      },
      lastPulledAt: 2,
    });

    const res = await pullChanges(["entries"], profile, { lastPulledAt: null });

    expect(res.changes.entries.created.length).toBe(0);
    expect(res.changes.entries.updated.length).toBe(0);
    expect(res.changes.entries.deleted.length).toBe(1);
    expect(res.changes.entries.deleted[0]).toBe("aaa");

    expect(res.timestamp).toBe(3);
  });

  test("counts rows correctly", async () => {
    const created = [];
    const updated = [];
    const deleted = [];
    const rowsPerType = 2480;

    for (let i = 0; i < rowsPerType; i++) {
      created.push({ id: i.toString(), data: "hello" });
    }

    const createChanges = await countChanges(
      {
        entries: { created, updated, deleted },
      },
      0,
    );
    expect(createChanges).toBe(rowsPerType);

    for (let i = 0; i < rowsPerType; i++) {
      updated.push({ id: i.toString(), data: "hello" });
    }
    const updateChanges = await countChanges(
      {
        entries: { created, updated, deleted },
      },
      0,
    );
    expect(updateChanges).toBe(rowsPerType * 2); // includes creates!

    for (let i = 0; i < rowsPerType; i++) {
      deleted.push(i.toString());
    }
    const deleteChanges = await countChanges(
      {
        entries: { created, updated, deleted },
      },
      deleted.length,
    );
    // Deletion changes have one additional write, which is the DeleteRecord doc
    expect(deleteChanges).toBe(rowsPerType * 3 + 1);
  });

  test("pushes batches correctly", async () => {
    const profile = doc(getFirestore(), "backups", "golf");
    const created = [];
    const totalCreated = 2480;

    for (let i = 0; i < totalCreated; i++) {
      created.push({ id: i.toString(), data: "hello" });
    }

    await pushChanges(profile, {
      changes: {
        entries: {
          created,
          updated: [],
          deleted: [],
        },
      },
      lastPulledAt: 1,
    });

    const batches = await getDocs(collection(profile, "melonBatches"));
    expect(batches.docs.length).toBe(1);

    const batchRef = batches.docs[0];
    const batchDoc = batchRef.data();
    expect(batchDoc.melonLatestRevision).toBe(1);

    const creates = await getDocs(collection(batchRef.ref, "entries"));
    expect(creates.docs.length).toBe(totalCreated);

    const res = await pullChanges(["entries"], profile, { lastPulledAt: null });

    expect(res.changes.entries.created.length).toBe(0);
    expect(res.changes.entries.updated.length).toBe(totalCreated);
    expect(res.changes.entries.deleted.length).toBe(0);
  });

  test("deletes rows from baseDoc", async () => {
    const profile = doc(getFirestore(), "backups", "hotel");

    await pushChanges(profile, {
      changes: {
        entries: {
          created: [
            { id: "aaa", data: "hello" },
            { id: "bbb", data: "hi" },
            { id: "ccc", data: "there" },
          ],
          updated: [],
          deleted: [],
        },
      },
      lastPulledAt: 1,
    });
    await pushChanges(profile, {
      changes: {
        entries: {
          created: [],
          updated: [],
          deleted: ["bbb"],
        },
      },
      lastPulledAt: 2,
    });

    const res = await pullChanges(["entries"], profile, { lastPulledAt: null });

    expect(res.changes.entries.created.length).toBe(0);
    expect(res.changes.entries.updated.length).toBe(2);
    expect(res.changes.entries.deleted.length).toBe(1);
    expect(res.changes.entries.deleted[0]).toBe("bbb");

    const delSnap = await getDoc(doc(profile, "entries", "bbb"));
    expect(delSnap.exists()).toBe(false);
  });

  test("deletes rows from batches", async () => {
    const profile = doc(getFirestore(), "backups", "india");
    const created = [];
    const totalCreated = 1001;

    for (let i = 0; i < totalCreated; i++) {
      created.push({ id: i.toString(), data: "hello" });
    }

    await pushChanges(profile, {
      changes: {
        entries: {
          created,
          updated: [],
          deleted: [],
        },
      },
      lastPulledAt: 1,
    });

    const mid = await pullChanges(["entries"], profile, { lastPulledAt: null });
    const batches = await getDocs(collection(profile, "melonBatches"));

    expect(batches.docs.length).toBe(1);
    expect(mid.changes.entries.updated.length).toBe(totalCreated);

    await pushChanges(profile, {
      changes: {
        entries: {
          created: [],
          updated: [],
          deleted: ["218"],
        },
      },
      lastPulledAt: mid.timestamp,
    });

    const dels = await getDocs(collection(profile, "melonDeletes"));

    expect(dels.docs.length).toBe(1);

    const res = await pullChanges(["entries"], profile, {
      lastPulledAt: mid.timestamp,
    });

    // First verify the pull returns the right results
    expect(res.changes.entries.created.length).toBe(0);
    expect(res.changes.entries.deleted.length).toBe(1);
    expect(res.changes.entries.updated.length).toBe(0);

    // Now check that firestore doesn't have 218 anymore
    const delSnap = await getDoc(doc(batches.docs[0].ref, "entries", "218"));
    expect(delSnap.exists()).toBe(false);

    // Finally verify that a pull from the beginning of time won't even show
    // 218 ever existed.
    const fullPull = await pullChanges(["entries"], profile, {
      lastPulledAt: null,
    });
    const entries = fullPull.changes.entries;

    expect(entries.created.length).toBe(0);
    expect(entries.deleted.length).toBe(1);
    expect(entries.updated.length).toBe(totalCreated - 1);
    expect(entries.updated.map(u => u.id)).not.toContain("218");
  });

  test("handles invalid firestore IDs correctly", async () => {
    const profile = doc(getFirestore(), "backups", "juliet");
    const unencodedID =
      "https://rss.art19.com/smartless-gid://art19-episode-locator";

    await pushChanges(profile, {
      changes: {
        entries: {
          created: [
            {
              id: unencodedID,
              data: "hello",
            },
          ],
          updated: [],
          deleted: [],
        },
      },
      lastPulledAt: 1,
    });
    const res = await pullChanges(["entries"], profile, { lastPulledAt: 1 });

    expect(res.changes.entries.created.length).toBe(0);
    expect(res.changes.entries.updated.length).toBe(1);
    expect(res.changes.entries.updated[0].id).toBe(unencodedID);
    expect(res.changes.entries.deleted.length).toBe(0);
    expect(res.timestamp).toBe(2);
  });
});

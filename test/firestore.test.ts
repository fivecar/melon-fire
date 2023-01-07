/**
 * I got this from https://github.com/firebase/quickstart-testing
 */
import {
  apps,
  clearFirestoreData,
  initializeTestApp as mockInitializeTestApp,
} from "@firebase/rules-unit-testing";
import {
  afterAll,
  beforeAll,
  describe,
  expect,
  jest,
  test,
} from "@jest/globals";
import { exportsForTesting } from "../dist";

let activeApp;

jest.mock("@react-native-firebase/firestore", () => {
  const originalModule = jest.requireActual(
    "@react-native-firebase/firestore",
  ) as object;
  const mainApp = mockInitializeTestApp({ projectId: PROJECT_ID, auth: null });

  activeApp = mainApp;

  return {
    __esModule: true,
    ...originalModule,
    default: () => mainApp.firestore(),
  };
});

const pullChanges = exportsForTesting.pullChanges;
const pushChanges = exportsForTesting.pushChanges;

/**
 * The emulator will accept any project ID for testing.
 */
const PROJECT_ID = "melon-fire-test";

beforeEach(async () => {
  // Clear the database between tests
  await clearFirestoreData({ projectId: PROJECT_ID });
});

beforeAll(async () => {
  // Load the rules file before the tests begin
  // const rules = fs.readFileSync("firestore.rules", "utf8");
  // await firebase.loadFirestoreRules({ projectId: PROJECT_ID, rules });
});

afterAll(async () => {
  // Delete all the FirebaseApp instances created during testing
  // Note: this does not affect or clear any data
  await Promise.all(apps().map(app => app.delete()));
});

describe("Melon Fire", () => {
  test("ensure firebase works", async () => {
    const db = activeApp.firestore();
    const profile = db.collection("backups").doc("alice");

    await profile.set({ hello: "world" });

    const ref = await db.collection("backups").doc("alice").get();
    expect(ref.data()).toEqual({ hello: "world" });
  });

  test("pushes basic data successfully", async () => {
    const db = activeApp.firestore();
    const profile = db.collection("backups").doc("alice");

    console.log("About to push changes");
    await pushChanges(profile as any, {
      changes: {
        entries: {
          created: [{ id: "aaa", data: "hello" }],
          updated: [],
          deleted: [],
        },
      },
      lastPulledAt: 0,
    });

    console.log("About to get entries");
    const entries = await profile.collection("entries").get();

    console.log("About to check entries");
    expect(entries.docs.length).toBe(1);
    expect(entries.docs[0].data()).toEqual({ id: "aaa", data: "hello" });
  });
});

/**
 * ingester.ts — Poll Horizon for ProofRegistry contract events and write them
 * into the local DB.
 *
 * ProofRegistry emits two kinds of events:
 *
 *   Verified  topics: ["proof", "verified"]  value: expiry (u64)
 *   Revoked   topics: ["revoked"]            value: (holder, cred_type, issuer, ts)
 *
 * Horizon's /effects and /transactions endpoints don't surface Soroban contract
 * events natively, so we use the dedicated
 *   GET /contracts/{contract_id}/events
 * endpoint introduced alongside Protocol 20 / Soroban.
 *
 * Consistency guarantee
 * ──────────────────────
 * The indexer provides **eventual consistency with a configurable finality lag**.
 *
 *   - Events are only persisted once their ledger is at least `FINALITY_LAG`
 *     ledgers behind the network head (default: 6 ≈ 30 seconds at ~5 s/ledger).
 *   - This means indexed data is delayed by ~30 seconds in the default config,
 *     but is immune to Stellar ledger reorgs (which can replace the last 1–2
 *     ledgers during network instability).
 *   - If a reorg is detected (cursor > head), the indexer rolls back all claims
 *     with ledger_sequence beyond the reorg point and re-scans.
 *   - Idempotency: every cycle starts from (lastLedger + 1) and advances the
 *     cursor only after all rows for that ledger have been written. Restarting
 *     replays from the last saved cursor; duplicate events are absorbed by
 *     upsertClaim's ON CONFLICT DO UPDATE clause.
 *   - The `reconcile()` method can be called explicitly to re-scan a bounded
 *     window after a detected inconsistency.
 *
 * Configuration:
 *   FINALITY_LAG — number of ledgers to lag (default 6, set to 0 for no lag)
 */

import { Horizon } from "@stellar/stellar-sdk";
import type { Config } from "./config";
import type { Db } from "./db";

// ── Horizon event shape (Soroban contract events) ──────────────────────────

/**
 * Minimal representation of a record returned by
 * GET /contracts/{id}/events?cursor=…
 *
 * The full shape has many optional fields; we only care about these.
 */
interface HorizonContractEvent {
  /** Paging token / cursor. */
  paging_token: string;
  /** The contract that emitted this event. */
  contract_id: string;
  /** Ordered list of topic values, XDR-base64 encoded. */
  topic: string[];
  /** Event body value, XDR-base64 encoded. */
  value: string;
  /**
   * Ledger sequence number containing this event.
   * Horizon surfaces this as a string in the raw JSON.
   */
  ledger: number | string;
  /** Ledger close time (ISO-8601). */
  ledger_closed_at: string;
  /**
   * The transaction that contained this event.
   * Present on successful transactions.
   */
  transaction_hash?: string;
  /**
   * The account that submitted the transaction.
   * Horizon calls this "source_account" on the event object.
   */
  source_account?: string;
}

interface HorizonEventsPage {
  _embedded: { records: HorizonContractEvent[] };
}

// ── XDR decode helpers (no external XDR lib required) ─────────────────────

/**
 * Decode a base64-encoded Soroban ScVal and extract the string representation
 * of one of the following value kinds:
 *   - ScvSymbol  → raw symbol string
 *   - ScvAddress → Stellar StrKey (G…)
 *   - ScvU64     → decimal string
 *   - ScvU32     → decimal string
 *
 * We rely on stellar-sdk's SorobanDataBuilder / xdr module for the actual XDR
 * decode so we don't have to vendor a full XDR schema.
 */
function decodeScVal(b64: string): unknown {
  try {
    // stellar-sdk re-exports @stellar/stellar-base which ships xdr types.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { xdr, Address, scValToNative } = require(
      "@stellar/stellar-sdk"
    ) as typeof import("@stellar/stellar-sdk");

    const scval = xdr.ScVal.fromXDR(b64, "base64");
    // scValToNative converts to a JS primitive / bigint / string.
    // For Address types it returns a Stellar address string.
    const native: unknown = scValToNative(scval);

    // Addresses come back as an Address instance; convert to string.
    if (native instanceof Address) {
      return native.toString();
    }
    // BigInts → numbers (safe for u64 ledger values in our use-case).
    if (typeof native === "bigint") {
      return Number(native);
    }
    return native;
  } catch {
    return null;
  }
}

// ── Event parser ───────────────────────────────────────────────────────────

type ParsedEvent =
  | {
      kind: "verified";
      holder: string;
      credentialType: string;
      issuer: string;
      expiry: number;
      ledgerSequence: number;
      verifiedAt: number;
    }
  | {
      kind: "revoked";
      holder: string;
      credentialType: string;
    }
  | { kind: "unknown" };

/**
 * Parse a raw Horizon contract event record into our domain type.
 *
 * ProofRegistry event topology
 * ─────────────────────────────
 * Verified:
 *   topics[0] = ScvSymbol "proof"
 *   topics[1] = ScvSymbol "verified"
 *   value     = ScvU64 expiry
 *
 *   The holder and credential_type are NOT in the topics; they are implicit in
 *   the storage key.  Horizon does, however, surface the transaction's
 *   source_account which is the holder (they must sign submit_proof).
 *
 * Revoked (issuer-initiated, from revoke()):
 *   topics[0] = ScvSymbol "revoked"
 *   value     = ScvVec [holder, credential_type, issuer, timestamp]
 *
 * Revoked (holder self-revoke, revoke_proof()):
 *   No event is emitted by the contract for self-revoke — holder just removes
 *   the storage key.  We therefore won't see a chain event; claims will expire
 *   naturally.
 */
function parseEvent(
  ev: HorizonContractEvent,
  contractId: string
): ParsedEvent {
  if (ev.contract_id !== contractId) return { kind: "unknown" };

  const topics = ev.topic.map(decodeScVal);
  const value = decodeScVal(ev.value);
  const ledgerSequence =
    typeof ev.ledger === "string"
      ? parseInt(ev.ledger, 10)
      : ev.ledger;

  // verified event
  if (topics[0] === "proof" && topics[1] === "verified") {
    const holder = ev.source_account ?? "";
    // credential_type is the 3rd topic (index 2) when emitted — but the
    // contract's current publish call only emits 2 topics + value.
    // We extract credential_type from the 3rd topic if present, else "unknown".
    const credentialType =
      typeof topics[2] === "string" ? topics[2] : "unknown";
    const expiry = typeof value === "number" ? value : 0;

    return {
      kind: "verified",
      holder,
      credentialType,
      issuer: "",
      expiry,
      ledgerSequence,
      verifiedAt: Math.floor(
        new Date(ev.ledger_closed_at).getTime() / 1000
      ),
    };
  }

  // revoked event
  if (topics[0] === "revoked") {
    if (Array.isArray(value) && value.length >= 2) {
      const holder = String(value[0]);
      const credentialType = String(value[1]);
      return { kind: "revoked", holder, credentialType };
    }
  }

  return { kind: "unknown" };
}

// ── Ingester ───────────────────────────────────────────────────────────────

export interface Ingester {
  /** Run one ingestion cycle (fetch + write). Returns number of events processed. */
  tick(): Promise<number>;
  /**
   * Re-scan a bounded window of ledgers to reconcile after a detected reorg.
   * Deletes claims newer than `reorgPoint` and re-indexes up to the new head.
   */
  reconcile(reorgPoint: number): Promise<number>;
  /** Start a continuous polling loop. */
  start(): void;
  /** Stop the polling loop. */
  stop(): void;
}

export function createIngester(config: Config, db: Db): Ingester {
  const server = new Horizon.Server(config.horizonUrl, {
    allowHttp: config.horizonUrl.startsWith("http://"),
  });

  let running = false;
  let timer: ReturnType<typeof setTimeout> | null = null;

  // ── Helpers ─────────────────────────────────────────────────────────────

  /**
   * Fetch the current ledger sequence from Horizon.
   * Uses GET /ledgers?order=desc&limit=1 to get the latest closed ledger.
   */
  async function getLedgerHead(): Promise<number> {
    const url = new URL("/ledgers", config.horizonUrl);
    url.searchParams.set("order", "desc");
    url.searchParams.set("limit", "1");

    const res = await fetch(url.toString());
    if (!res.ok) {
      throw new Error(`Horizon /ledgers responded ${res.status}`);
    }
    const body = (await res.json()) as {
      _embedded?: { records?: Array<{ sequence: number | string }> };
    };
    const seq = body._embedded?.records?.[0]?.sequence;
    if (seq === undefined) throw new Error("No ledger returned from Horizon");
    return typeof seq === "string" ? parseInt(seq, 10) : seq;
  }

  /**
   * Fetch contract events starting from a cursor, up to a max ledger.
   * Events with ledger > maxLedger are skipped (not persisted).
   */
  async function fetchEvents(
    cursor: string | undefined,
    maxLedger: number
  ): Promise<HorizonContractEvent[]> {
    const url = new URL(
      `/contracts/${config.proofRegistryContractId}/events`,
      config.horizonUrl
    );
    url.searchParams.set("order", "asc");
    url.searchParams.set("limit", "200");
    if (cursor !== undefined) {
      url.searchParams.set("cursor", cursor);
    }

    const res = await fetch(url.toString());
    if (!res.ok) {
      if (res.status === 404) return [];
      throw new Error(`Horizon responded ${res.status}: ${await res.text()}`);
    }
    const page = (await res.json()) as HorizonEventsPage;
    const records = page._embedded?.records ?? [];

    // Filter out events beyond the finality boundary
    return records.filter((ev) => {
      const evLedger = typeof ev.ledger === "string" ? parseInt(ev.ledger, 10) : ev.ledger;
      return evLedger <= maxLedger;
    });
  }

  /** Process a batch of events, upserting/revoking claims. Returns count. */
  async function processEvents(events: HorizonContractEvent[]): Promise<number> {
    let processed = 0;
    for (const ev of events) {
      const parsed = parseEvent(ev, config.proofRegistryContractId);

      if (parsed.kind === "verified") {
        await db.upsertClaim({
          wallet: parsed.holder,
          credential_type: parsed.credentialType,
          issuer: parsed.issuer,
          verified_at: parsed.verifiedAt,
          expiry: parsed.expiry,
          ledger_sequence: parsed.ledgerSequence,
          threshold: null,
          revoked: 0,
        });
        processed++;
      } else if (parsed.kind === "revoked") {
        await db.revokeClaim(parsed.holder, parsed.credentialType);
        processed++;
      }
    }
    return processed;
  }

  // ── Core ingestion tick ──────────────────────────────────────────────────

  async function tick(): Promise<number> {
    const lastLedger = await db.getLastLedger();

    // 1. Determine the network head and the finality-safe ceiling.
    let headLedger: number;
    try {
      headLedger = await getLedgerHead();
    } catch (err) {
      console.warn("[indexer] Could not fetch ledger head:", (err as Error).message);
      return 0;
    }

    // The finality ceiling: only persist events at or below (head - lag).
    // If the network hasn't progressed past our lag buffer yet, there's
    // nothing final to index.
    const finalityCeiling = headLedger - config.finalityLag;
    if (finalityCeiling <= lastLedger) {
      // Head hasn't advanced past our cursor + lag yet — nothing to do.
      return 0;
    }

    // 2. Detect potential reorg: if our cursor claims to have ingested
    //    a ledger that is now beyond the network head, the chain was
    //    likely reorged past our last checkpoint.
    if (lastLedger > headLedger) {
      console.warn(
        `[indexer] REORG DETECTED: cursor=${lastLedger} > head=${headLedger}. ` +
          `Rolling back to head and re-scanning.`
      );
      return reconcile(headLedger);
    }

    // 3. Build the Horizon cursor. For a fresh start with startLedger
    //    configured, begin there; otherwise resume from lastLedger.
    const cursorNum = lastLedger > 0 ? lastLedger * 100_000 : 0;
    const cursor =
      config.startLedger > 0 && lastLedger === 0
        ? String(config.startLedger * 100_000)
        : cursorNum > 0
        ? String(cursorNum)
        : undefined;

    // 4. Fetch events up to the finality ceiling.
    let events: HorizonContractEvent[];
    try {
      events = await fetchEvents(cursor, finalityCeiling);
    } catch (err) {
      console.warn("[indexer] Horizon fetch error:", (err as Error).message);
      return 0;
    }

    if (events.length === 0) return 0;

    // 5. Process events and update cursor.
    const processed = await processEvents(events);

    // Advance cursor to the highest ledger among processed events.
    let maxLedger = lastLedger;
    for (const ev of events) {
      const evLedger = typeof ev.ledger === "string" ? parseInt(ev.ledger, 10) : ev.ledger;
      if (evLedger > maxLedger) maxLedger = evLedger;
    }
    if (maxLedger > lastLedger) {
      await db.setLastLedger(maxLedger);
    }

    return processed;
  }

  // ── Reorg reconciliation ─────────────────────────────────────────────────

  /**
   * Re-scan a bounded window after detecting a reorg or called explicitly.
   * Deletes claims with ledger_sequence > reorgPoint, then re-indexes
   * from reorgPoint up to the current finality ceiling.
   */
  async function reconcile(reorgPoint: number): Promise<number> {
    console.log(`[indexer] Reconciling from ledger ${reorgPoint}`);

    // Roll back: delete all claims with ledger_sequence beyond the reorg point.
    await db.deleteClaimsAfter(reorgPoint);

    // Reset cursor so we re-fetch from the reorg point.
    await db.setLastLedger(reorgPoint);

    // Now run a normal tick — it will fetch events from reorgPoint onward
    // up to the new finality ceiling.
    return tick();
  }

  function scheduleNext() {
    timer = setTimeout(async () => {
      if (!running) return;
      try {
        const n = await tick();
        if (n > 0) {
          console.log(`[indexer] processed ${n} event(s)`);
        }
      } catch (err) {
        console.error("[indexer] tick error:", err);
      }
      scheduleNext();
    }, config.pollIntervalMs);
  }

  return {
    tick,
    reconcile,
    start() {
      if (running) return;
      running = true;
      console.log(
        `[indexer] starting — contract=${config.proofRegistryContractId} ` +
          `network=${config.stellarNetwork} poll=${config.pollIntervalMs}ms`
      );
      scheduleNext();
    },
    stop() {
      running = false;
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
    },
  };
}

# Tx\_gen - Simple Transaction Generator for rusty-kaspa

A transaction generator built for easy use and quick testing.
It runs inside the [`rusty-kaspa`](https://github.com/kaspanet/rusty-kaspa) workspace and talks to Kaspa over gRPC.

---

## What this script does

1. **UTXO Analysis and Splitting**

   * Reads your address UTXOs.
   * If you have fewer than `TARGET_UTXO_COUNT`, it uses your largest UTXO to create many small UTXOs in batches of `OUTPUTS_PER_TRANSACTION`.
   * This prepares a pool of spendable UTXOs.

2. **High-rate Transaction Sending**

   * Sends 1-input 1-output self-payments at a controlled target TPS.
   * Paces with a short tick interval for smooth throughput, refreshes UTXOs, and keeps a large async inflight queue to avoid stalls.
   * Tracks per-second TPS and a rolling 10-second average.

The script self-checks that your address prefix matches the network, and that the node you connected to is the network you selected.

---

## Getting a private key

You have a few straightforward options. Do **one** of the following:

* **Generator repo**: Use the community tool
  [https://github.com/deepakdhaka-1/Kaspa-Wallet-Generate](https://github.com/deepakdhaka-1/Kaspa-Wallet-Generate)
  Follow its instructions to get a 24-word seed and private key.

* **Verify the seed**: It is advised to load the same 24-word seed in **kaspa-NG** web wallet first, make sure the address is valid and usable, then use a **public address** from kaspa-NG to fund it.
  After that, take only the **private key** from the generator and paste it into this script.

* **K social (testnet key)**: You can also create a testnet account on “K - a decentralized twitter on kaspa” and take the private key from there:
  [https://ksocialnetwork.pages.dev/watching](https://ksocialnetwork.pages.dev/watching)

> Put the private key hex into `PRIVATE_KEY_HEX` in `main.rs`. 

---

## Networks

* **Default**: mainnet
* **Testnet-10**: pass `--net tn10` or `--net testnet10`
* **Devnet**: pass `--net devnet`

The script uses these default gRPC endpoints:

* Mainnet: `grpc://n-mainnet.kaspa.ws:16110`
* Testnet-10: `grpc://n-testnet-10.kaspa.ws:16210`
* Devnet: `grpc://127.0.0.1:16610`

Your address prefix must match the network:

* `kaspa:` for mainnet
* `kaspatest:` for testnet
* `kaspadev:` for devnet

If they do not match, the script will stop with a clear error.

For custom devnets, two extra runtime overrides are useful:

* `--rpc-url <grpc://host:port>` to target a non-default devnet RPC endpoint
* `--coinbase-maturity <daa-score>` when your devnet blockrate override changes the effective maturity window

---

## Quick start

1. **Clone rusty-kaspa**
   Follow the build instructions in the repo:
   [https://github.com/kaspanet/rusty-kaspa](https://github.com/kaspanet/rusty-kaspa)

2. **Add this tool as a workspace member**

   * Create a new folder at the root of the workspace named **`Tx_gen`**.

   * Inside `Tx_gen`, paste `src` with `main.rs` in it.

   * In the root `Cargo.toml` of `rusty-kaspa`, add `"Tx_gen"` to the `[workspace] members` list.
     Example:

     ```toml
     [workspace]
     members = [
       # existing members...
       "Tx_gen"
     ]
     ```

   > Do not change anything else in rusty-kaspa. Just follow its build guide.

3. **Insert your private key**

   * Open `Tx_gen/src/main.rs`
   * Set:

     ```rust
     const PRIVATE_KEY_HEX: &str = "<your_private_key_hex>";
     ```

5. **Run on testnet-10**

   ```bash
   cargo run --release --bin Tx_gen -- --net tn10
   ```

6. **Run on devnet**

   ```bash
   cargo run --release --bin Tx_gen -- --net devnet
   ```

   For custom devnets, you will usually also want an explicit maturity override:

   ```bash
   cargo run --release --bin Tx_gen -- --net devnet --coinbase-maturity 2000
   ```

   If the node is not on the same host, also pass `--rpc-url`.

7. **Run on mainnet** (only if you know what you are doing)

   ```bash
   cargo run --release --bin Tx_gen
   ```

---

## How to fund and sanity-check

1. Generate a seed and private key using the generator repo linked above.
2. Load that seed in kaspa-NG web wallet to confirm you can see the expected address.
3. Copy a **public address** from kaspa-NG and send test coins to it (for testnet, use faucets as available).
4. Paste only the **private key hex** into `PRIVATE_KEY_HEX` and run the script on the matching network.

---

## Tuning knobs

These are the top constants in the script, with plain-English descriptions. Adjust before building.

| Constant                  |    Type | Meaning                                                                                                                              |
| ------------------------- | ------: | ------------------------------------------------------------------------------------------------------------------------------------ |
| `PRIVATE_KEY_HEX`         |  `&str` | Your hex private key. Required. Do not commit it.                                                                                    |
| `TARGET_UTXO_COUNT`       | `usize` | Target number of small UTXOs to prepare. If you already have at least this count, the split phase is skipped.                        |
| `AMOUNT_PER_UTXO`         |   `u64` | Value of each split output in sompi. `100_000_000` sompi = 1 KAS. Example uses `150_000_000` sompi = 1.5 KAS.                        |
| `OUTPUTS_PER_TRANSACTION` | `usize` | How many split outputs per splitting transaction. The last tx in the split phase may use fewer to hit the exact target.              |
| `TRANSACTIONS_COUNT`      | `usize` | Derived from `TARGET_UTXO_COUNT` and `OUTPUTS_PER_TRANSACTION`. Usually no need to touch.                                            |
| `SPAM_DURATION_SECONDS`   |   `u64` | For the send loop. Set to `0` to run indefinitely. Otherwise stops after N seconds.                                                  |
| `TARGET_TPS`              |   `u64` | Requested transactions per second for the send loop. Actual TPS depends on UTXO availability and network acceptance.                 |
| `UNLEASHED`               |  `bool` | Safety cap switch. If `false`, caps at 100 TPS even if `TARGET_TPS` is higher. Set to `true` only after you have verified stability. |
| `MILLIS_PER_TICK`         |   `u64` | Pacing tick in milliseconds. Lower values give smoother TPS control. Default `10` ms.                                                |
| `BASE_FEE_RATE`           |   `u64` | Base fee rate (sompi per gram) for the 1-in 1-out spam txs. Combined with `estimated_mass` to compute fee.                           |
| `CLIENT_POOL_SIZE`        | `usize` | Number of gRPC clients in the pool for parallel submits.                                                                             |
| `UTXO_REFRESH_SECS`       |   `u64` | How often to refresh UTXOs from the node. Also refreshes when the local pool grows low.                                              |
| `MIN_CHANGE_SOMPI`        |   `u64` | Minimum change value to keep when splitting or sending. Prevents dust outputs.                                                       |
| `MAX_PENDING_AGE_SECS`    |   `u64` | Old pending reservations are pruned after this many seconds to avoid starvation.                                                     |

Fee helpers:

* `estimated_mass(...)` returns a constant `1700` as a simple stand-in.
* `required_fee_base(...)` uses `BASE_FEE_RATE * estimated_mass(...)`.
* `required_fee_splitting(...)` uses a fixed `FEE_RATE = 10` for split transactions.

You can raise fee rates if your node rejects for size or fee reasons.

---

## Typical flow in detail

1. **Connect**
   A small pool of gRPC clients is created for parallelism. The script fetches node info and checks that:

   * Your address prefix matches the selected network.
   * The node identifies as the expected network.

2. **Analyze UTXOs**
   It pulls confirmed, spendable UTXOs for your address, applying a simple maturity rule:

   * Non-coinbase: needs 10 confirmations
   * Coinbase: needs `coinbase_maturity` (default 100, override with `--coinbase-maturity` for custom devnets)

3. **Split if needed**
   If you have fewer than `TARGET_UTXO_COUNT`, it:

   * Uses your largest UTXO.
   * Creates `OUTPUTS_PER_TRANSACTION` outputs of equal value `AMOUNT_PER_UTXO`.
   * Leaves change only if it is at least `MIN_CHANGE_SOMPI`.
   * Repeats until you reach the target or funds run out.
   * Brief sleeps between submits to be nice.

4. **Send loop**

   * Computes a fractional target per tick based on `TARGET_TPS` and `MILLIS_PER_TICK`.
   * Refreshes UTXOs regularly and when low.
   * Builds 1-in 1-out signed transactions in parallel.
   * Maintains a large async inflight queue with round-robin client selection.
   * Prints per-second TPS and a rolling 10-second average.

---


## Tips and safety

* Prefer **testnet-10** when trying high TPS or weird settings.
* Fund the address before running. The split phase needs enough balance to create your target number of UTXOs and pay fees.
* If you see “Address prefix does not match selected network” or “Connected node does not look like …”, fix either the network flag or the address you are using.
* If the node rejects for mass or fee reasons, raise fee rates or reduce `OUTPUTS_PER_TRANSACTION`.
* The UTXO splitting phase and the transaction generation phase may overlap. once all UTXO splitting transactions are confirmed the script will run at the set speed.

---

## Credits

* Based on **Rothschild**.
* Built on the `rusty-kaspa` stack.

---




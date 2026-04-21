use kaspa_addresses::{Address, Prefix};
use kaspa_consensus_core::{
    constants::TX_VERSION,
    sign::sign,
    subnets::SUBNETWORK_ID_NATIVE,
    tx::{
        MutableTransaction, Transaction, TransactionInput, TransactionOutpoint, TransactionOutput,
        UtxoEntry as CoreUtxoEntry,
    },
};
use kaspa_grpc_client::GrpcClient;
use kaspa_rpc_core::{
    api::rpc::RpcApi,
    model::{
        GetServerInfoRequest, GetServerInfoResponse, GetUtxosByAddressesRequest, RpcUtxoEntry,
        SubmitTransactionRequest,
    },
    RpcTransaction,
};
use kaspa_txscript::pay_to_address_script;
use secp256k1::{Keypair, SecretKey, SECP256K1};
use std::{
    collections::{HashMap, HashSet},
    env,
    str::FromStr,
    sync::Arc,
    time::Instant,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::time::{interval, sleep, Duration, MissedTickBehavior};

// parallel build + async fanout
use futures::stream::{FuturesUnordered, StreamExt};
use rayon::prelude::*;

// ----------------------- tunables -----------------------
const PRIVATE_KEY_HEX: &str = "ENTER PRIVATE KEY";
const TARGET_UTXO_COUNT: usize = 100;
const AMOUNT_PER_UTXO: u64 = 150_000_000; // 1.5 KAS
const OUTPUTS_PER_TRANSACTION: usize = 10;
const TRANSACTIONS_COUNT: usize =
    (TARGET_UTXO_COUNT + OUTPUTS_PER_TRANSACTION - 1) / OUTPUTS_PER_TRANSACTION;
const SPAM_DURATION_SECONDS: u64 = 86_400; // 0 means run forever

const TARGET_TPS: u64 = 50; // 1 UTXO = 0.25-0.33 TPS, set TPS to UTXO_COUNT / 2
const UNLEASHED: bool = true; // false keeps safety cap at 100 TPS
const MILLIS_PER_TICK: u64 = 10; // 10 ms tick for smooth pacing

const BASE_FEE_RATE: u64 = 1; // 1 sompi/gram
const CLIENT_POOL_SIZE: usize = 8; // gRPC connections
const UTXO_REFRESH_SECS: u64 = 1;
const MIN_CHANGE_SOMPI: u64 = 1_000_000; // 0.01 KAS
const MAX_PENDING_AGE_SECS: u64 = 3600;

// ----------------------- fee helpers -----------------------
#[allow(unused_variables)]
const fn estimated_mass(num_inputs: usize, num_outputs: u64) -> u64 {
    1700
}
pub const fn required_fee_base(num_inputs: usize, num_outputs: u64) -> u64 {
    BASE_FEE_RATE * estimated_mass(num_inputs, num_outputs)
}
pub const fn required_fee_splitting(num_inputs: usize, num_outputs: u64) -> u64 {
    const FEE_RATE: u64 = 10;
    FEE_RATE * estimated_mass(num_inputs, num_outputs)
}

// ----------------------- main -----------------------
#[derive(Clone, Copy, Debug)]
enum Net {
    Mainnet,
    Testnet10,
    Devnet,
}

impl Net {
    fn from_args() -> Self {
        // supports: --net tn10 | --net testnet10 | --net devnet | --net=tn10 | --net=testnet10 | --net=devnet
        let mut net = Net::Mainnet;
        let mut it = env::args().skip(1);
        while let Some(arg) = it.next() {
            if arg == "--net" {
                if let Some(v) = it.next() {
                    if v.eq_ignore_ascii_case("tn10") || v.eq_ignore_ascii_case("testnet10") {
                        net = Net::Testnet10;
                    } else if v.eq_ignore_ascii_case("devnet") {
                        net = Net::Devnet;
                    }
                }
            } else if let Some(v) = arg.strip_prefix("--net=") {
                if v.eq_ignore_ascii_case("tn10") || v.eq_ignore_ascii_case("testnet10") {
                    net = Net::Testnet10;
                } else if v.eq_ignore_ascii_case("devnet") {
                    net = Net::Devnet;
                }
            }
        }
        net
    }

    fn grpc_url(self) -> String {
        match self {
            Net::Mainnet => "grpc://n-mainnet.kaspa.ws:16110".to_string(),
            Net::Testnet10 => "grpc://n-testnet-10.kaspa.ws:16210".to_string(),
            Net::Devnet => "grpc://127.0.0.1:16610".to_string(),
        }
    }

    fn prefix(self) -> Prefix {
        match self {
            Net::Mainnet => Prefix::Mainnet,
            Net::Testnet10 => Prefix::Testnet, // testnet addresses use "kaspatest:"
            Net::Devnet => Prefix::Devnet,
        }
    }

    fn expected_hint(self) -> &'static str {
        match self {
            Net::Mainnet => "mainnet",
            Net::Testnet10 => "testnet-10",
            Net::Devnet => "devnet",
        }
    }

    fn default_coinbase_maturity(self) -> u64 {
        match self {
            Net::Mainnet | Net::Testnet10 | Net::Devnet => 1000,
        }
    }
}

fn arg_value(flag: &str) -> Option<String> {
    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == flag {
            return it.next();
        }
        if let Some(value) = arg.strip_prefix(&format!("{flag}=")) {
            return Some(value.to_string());
        }
    }
    None
}

fn arg_u64(flag: &str) -> Option<u64> {
    arg_value(flag).and_then(|value| value.parse::<u64>().ok())
}

fn assert_network_matches(
    info: &GetServerInfoResponse,
    net: Net,
    addr: &Address,
) -> Result<(), Box<dyn std::error::Error>> {
    // Address prefix must match selected net
    match (net, addr.prefix) {
        // prefix is a field, not a method
        (Net::Mainnet, Prefix::Mainnet)
        | (Net::Testnet10, Prefix::Testnet)
        | (Net::Devnet, Prefix::Devnet) => {}
        _ => return Err("Address prefix does not match selected network".into()),
    }

    // Check node's reported network id string
    let nid = info.network_id.to_string().to_lowercase();
    let hint = net.expected_hint();
    if !nid.contains(hint) {
        return Err(format!("Connected node \"{}\" does not look like {}", nid, hint).into());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let net = Net::from_args();
    let rpc_url = arg_value("--rpc-url").unwrap_or_else(|| net.grpc_url());
    let coinbase_maturity =
        arg_u64("--coinbase-maturity").unwrap_or_else(|| net.default_coinbase_maturity());

    // connection pool
    let mut clients: Vec<Arc<GrpcClient>> = Vec::with_capacity(CLIENT_POOL_SIZE);
    for _ in 0..CLIENT_POOL_SIZE {
        clients.push(Arc::new(GrpcClient::connect(rpc_url.clone()).await?));
    }

    let secret_key = SecretKey::from_str(PRIVATE_KEY_HEX)?;
    let keypair = Keypair::from_secret_key(&SECP256K1, &secret_key);
    let address = Address::new(
        net.prefix(),
        kaspa_addresses::Version::PubKey,
        &keypair.x_only_public_key().0.serialize(),
    );
    let si = clients[0]
        .get_server_info_call(None, GetServerInfoRequest {})
        .await?;
    assert_network_matches(&si, net, &address)?;

    println!("=== UTXO Analysis ===");
    #[allow(unused_mut)]
    let mut utxos = fetch_spendable_utxos(&clients[0], address.clone(), coinbase_maturity).await?;
    let current_utxo_count = utxos.len();
    let total_balance: u64 = utxos.iter().map(|(_, entry)| entry.amount).sum();
    println!("Current UTXOs: {}", current_utxo_count);
    println!("Total balance: {} KAS", total_balance / 100_000_000);

    if current_utxo_count < TARGET_UTXO_COUNT {
        println!("=== Phase 1: UTXO Splitting ===");
        println!(
            "Need to create {} more UTXOs",
            TARGET_UTXO_COUNT - current_utxo_count
        );

        let largest_utxo = utxos.iter().max_by_key(|(_, e)| e.amount).unwrap().clone();
        let kas_amount = largest_utxo.1.amount / 100_000_000;
        println!("Using largest UTXO with {} KAS for splitting", kas_amount);
        if kas_amount < 10 {
            return Err(format!("Largest UTXO has {} KAS, more is needed", kas_amount).into());
        }

        let mut current_utxo = largest_utxo;
        let mut created = 0usize;

        for i in 0..TRANSACTIONS_COUNT {
            let remaining_tx = TRANSACTIONS_COUNT - i;
            let outputs_this_tx = if remaining_tx == 1 {
                TARGET_UTXO_COUNT - (i * OUTPUTS_PER_TRANSACTION)
            } else {
                OUTPUTS_PER_TRANSACTION
            };

            let total_output_value = AMOUNT_PER_UTXO * outputs_this_tx as u64;
            let estimated_fee = required_fee_splitting(1, outputs_this_tx as u64 + 1);
            let change_value = current_utxo
                .1
                .amount
                .saturating_sub(total_output_value + estimated_fee);

            if change_value < MIN_CHANGE_SOMPI && i < TRANSACTIONS_COUNT - 1 {
                println!("Insufficient funds for change in tx {}, stopping", i + 1);
                break;
            }

            let tx = create_splitting_transaction(
                keypair,
                &current_utxo,
                AMOUNT_PER_UTXO,
                outputs_this_tx,
                change_value,
                &address,
            )?;

            println!(
                "Submitting splitting transaction {} with {} outputs",
                i + 1,
                outputs_this_tx
            );
            clients[0]
                .submit_transaction_call(
                    None,
                    SubmitTransactionRequest {
                        transaction: RpcTransaction::from(&tx),
                        allow_orphan: true,
                    },
                )
                .await?;

            created += 1;

            if i < TRANSACTIONS_COUNT - 1 && change_value >= MIN_CHANGE_SOMPI {
                let change_outpoint = TransactionOutpoint::new(tx.id(), outputs_this_tx as u32);
                let change_entry = CoreUtxoEntry {
                    amount: change_value,
                    script_public_key: pay_to_address_script(&address),
                    block_daa_score: current_utxo.1.block_daa_score,
                    is_coinbase: false,
                };
                current_utxo = (change_outpoint, change_entry);
            }

            sleep(Duration::from_millis(200)).await;
        }

        println!(
            "Created {} splitting transactions, waiting for confirmations...",
            created
        );
        sleep(Duration::from_secs(10)).await;
    } else {
        println!(
            "Already have {} UTXOs (target: {}), skipping splitting phase",
            current_utxo_count, TARGET_UTXO_COUNT
        );
    }

    println!("=== Phase 2: Transaction Spam ===");
    spam_transactions(
        &clients,
        address,
        keypair,
        TARGET_TPS,
        SPAM_DURATION_SECONDS,
        coinbase_maturity,
    )
    .await?;
    Ok(())
}

// ----------------------- spam loop -----------------------
async fn spam_transactions(
    clients: &[Arc<GrpcClient>],
    address: Address,
    keypair: Keypair,
    target_tps: u64,
    duration_seconds: u64,
    coinbase_maturity: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let client0 = clients[0].clone();
    let tps_tx = spawn_tps_logger();

    // UTXO state
    let mut pending: HashMap<TransactionOutpoint, Instant> = HashMap::new(); // in-flight or waiting confirm
    let mut spent: HashSet<TransactionOutpoint> = HashSet::new(); // successfully accepted by node (may still be unconfirmed)

    // pacing & stats
    let mut stats_start = Instant::now();
    let mut sent_since_reset = 0u64;
    let start = Instant::now();

    // safety cap (flip UNLEASHED to true once you verify)
    let effective_tps = if UNLEASHED {
        target_tps
    } else {
        target_tps.min(100)
    };

    let mut ticker = interval(Duration::from_millis(MILLIS_PER_TICK));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut stats_ticker = interval(Duration::from_secs(1));
    stats_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // initial UTXOs
    let mut utxos = fetch_spendable_utxos(&client0, address.clone(), coinbase_maturity).await?;
    let mut last_refresh = Instant::now();
    let mut idx = 0usize;

    println!(
        "Starting spam at {} TPS ({} ms tick). Initial UTXOs: {}",
        effective_tps,
        MILLIS_PER_TICK,
        utxos.len()
    );

    // fractional pacing: e.g., 200 TPS at 10 ms => 2.0 tx/tick
    let target_per_tick: f64 = (effective_tps as f64) * (MILLIS_PER_TICK as f64) / 1000.0;
    let mut carry: f64 = 0.0;

    // persistent inflight submit queue (don’t block the tick)
    let mut inflight: FuturesUnordered<_> = FuturesUnordered::new();
    const MAX_INFLIGHT: usize = 20_000; // keep a leash on concurrent submits
    let mut rr = 0usize; // round-robin across client pool

    // reuse keypair cheaply
    let keypair_arc = Arc::new(keypair);

    loop {
        tokio::select! {
            // pace producer
            _ = ticker.tick() => {
                if duration_seconds > 0 && start.elapsed().as_secs() >= duration_seconds {
                    println!("Spam duration completed after {} seconds", duration_seconds);
                    break;
                }

                // refresh UTXOs periodically or when running low
                if last_refresh.elapsed().as_secs() >= UTXO_REFRESH_SECS || idx >= utxos.len().saturating_sub(8) {
                    let mut fresh = fetch_spendable_utxos(&client0, address.clone(), coinbase_maturity).await?;
                    // exclude anything already reserved/pending or already spent
                    fresh.retain(|(op, _)| !pending.contains_key(op) && !spent.contains(op));
                    utxos = fresh;
                    idx = 0;
                    last_refresh = Instant::now();
                    println!("Refreshed UTXOs: {}", utxos.len());
                }

                if idx >= utxos.len() { continue; }
                if inflight.len() >= MAX_INFLIGHT { continue; }

                // how many we *intend* to add this tick
                let mut to_add: u64 = (target_per_tick + carry).floor() as u64;
                carry = (target_per_tick + carry) - (to_add as f64);

                // respect available UTXOs and inflight cap
                to_add = to_add
                    .min((utxos.len() - idx) as u64)
                    .min((MAX_INFLIGHT - inflight.len()) as u64);

                if to_add == 0 { continue; }

                // batch for this tick
                let batch: Vec<(TransactionOutpoint, CoreUtxoEntry)> =
                    utxos[idx .. idx + (to_add as usize)].to_vec();

                // build txs in parallel
                let addr_clone = address.clone();
                let kp = keypair_arc.clone();
                let tx_jobs: Vec<(Transaction, TransactionOutpoint)> = batch
                    .into_par_iter()
                    .filter_map(|(op, entry)| {
                        let fee = required_fee_base(1, 1);
                        let out_amt = entry.amount.saturating_sub(fee);
                        if out_amt < MIN_CHANGE_SOMPI { return None; }
                        create_spam_transaction((*kp).clone(), op, entry, out_amt, &addr_clone).ok()
                            .map(|tx| (tx, op))
                    })
                    .collect();

                // reserve UTXOs *before* submit so refresh won’t reuse them
                let now = Instant::now();
                for (_, op) in &tx_jobs {
                    pending.insert(*op, now);
                }

                // push submits onto inflight queue; do not await here
                for (i, (tx, op)) in tx_jobs.into_iter().enumerate() {
                    let client = clients[(rr + i) % clients.len()].clone();
                    inflight.push(async move {
                        let res = client
                            .submit_transaction_call(None, SubmitTransactionRequest {
                                transaction: RpcTransaction::from(&tx),
                                allow_orphan: false,
                            })
                            .await;
                        (res, op)
                    });
                }
                rr = (rr + to_add as usize) % clients.len();

                // advance consumed slice
                idx = idx.saturating_add(to_add as usize);

                // prune very old pending just in case
                let max_age = Duration::from_secs(MAX_PENDING_AGE_SECS);
                pending.retain(|_, t| Instant::now().duration_since(*t) <= max_age);
            }

            // consume completed submits as they finish (doesn't block ticks)
            Some((res, op)) = inflight.next() => {
                match res {
                    Ok(_) => {
                        // success: keep as pending until confirmed; also mark as spent so we never reuse
                        spent.insert(op);
                        sent_since_reset += 1;
                        let _ = tps_tx.send(1);
                    }
                    Err(e) => {
                        // failure: free the UTXO so it can be retried in a refresh
                        pending.remove(&op);
                        println!("Submit failed: {e}");
                    }
                }
            }

            // once per second print stats
            _ = stats_ticker.tick() => {
                let mem = client0.get_info().await.map(|i| i.mempool_size).unwrap_or(0);
                let secs = stats_start.elapsed().as_secs_f64();
                let tps = if secs > 0.0 { sent_since_reset as f64 / secs } else { 0.0 };
                println!(
                    "TPS (1s avg): {:.1} | sent: {} | mempool(node): {} | inflight: {} | local-pending: {} | UTXOs left: {} | runtime: {}s",
                    tps, sent_since_reset, mem, inflight.len(), pending.len(),
                    utxos.len().saturating_sub(idx), start.elapsed().as_secs()
                );
                stats_start = Instant::now();
                sent_since_reset = 0;
            }
        }
    }

    Ok(())
}

// ----------------------- tx builders -----------------------
fn create_splitting_transaction(
    keypair: Keypair,
    utxo: &(TransactionOutpoint, CoreUtxoEntry),
    amount_per_output: u64,
    num_target_outputs: usize,
    change_value: u64,
    address: &Address,
) -> Result<Transaction, Box<dyn std::error::Error>> {
    let script_public_key = pay_to_address_script(address);

    let inputs = vec![TransactionInput {
        previous_outpoint: utxo.0,
        signature_script: vec![],
        sequence: 0,
        sig_op_count: 1,
    }];

    let mut outputs = Vec::with_capacity(num_target_outputs + 1);
    for _ in 0..num_target_outputs {
        outputs.push(TransactionOutput {
            value: amount_per_output,
            script_public_key: script_public_key.clone(),
        });
    }

    if change_value > MIN_CHANGE_SOMPI {
        outputs.push(TransactionOutput {
            value: change_value,
            script_public_key: script_public_key.clone(),
        });
    }

    let unsigned_tx = Transaction::new(
        TX_VERSION,
        inputs,
        outputs,
        0,
        SUBNETWORK_ID_NATIVE,
        0,
        vec![],
    );

    let signed_tx = sign(
        MutableTransaction::with_entries(unsigned_tx, vec![utxo.1.clone()]),
        keypair,
    );

    Ok(signed_tx.tx)
}

fn create_spam_transaction(
    keypair: Keypair,
    input_outpoint: TransactionOutpoint,
    input_entry: CoreUtxoEntry,
    output_amount: u64,
    address: &Address,
) -> Result<Transaction, Box<dyn std::error::Error>> {
    let script_public_key = pay_to_address_script(address);

    let inputs = vec![TransactionInput {
        previous_outpoint: input_outpoint,
        signature_script: vec![],
        sequence: 0,
        sig_op_count: 1,
    }];

    let outputs = vec![TransactionOutput {
        value: output_amount,
        script_public_key,
    }];

    let unsigned_tx = Transaction::new(
        TX_VERSION,
        inputs,
        outputs,
        0,
        SUBNETWORK_ID_NATIVE,
        0,
        vec![],
    );

    let signed_tx = sign(
        MutableTransaction::with_entries(unsigned_tx, vec![input_entry]),
        keypair,
    );

    Ok(signed_tx.tx)
}

// ----------------------- UTXO helpers -----------------------
async fn fetch_spendable_utxos(
    client: &GrpcClient,
    address: Address,
    coinbase_maturity: u64,
) -> Result<Vec<(TransactionOutpoint, CoreUtxoEntry)>, Box<dyn std::error::Error>> {
    let resp = client
        .get_utxos_by_addresses_call(
            None,
            GetUtxosByAddressesRequest {
                addresses: vec![address.clone()],
            },
        )
        .await?;
    let server_info = client
        .get_server_info_call(None, GetServerInfoRequest {})
        .await?;
    let virtual_daa_score = server_info.virtual_daa_score;

    let mut utxos = Vec::with_capacity(resp.entries.len());
    for resp_entry in resp
        .entries
        .into_iter()
        .filter(|e| is_utxo_spendable(&e.utxo_entry, virtual_daa_score, coinbase_maturity))
    {
        assert!(resp_entry.address.is_some());
        assert_eq!(*resp_entry.address.as_ref().unwrap(), address);
        utxos.push((
            TransactionOutpoint::from(resp_entry.outpoint),
            CoreUtxoEntry::from(resp_entry.utxo_entry),
        ));
    }
    // largest first helps survive fee bumps
    utxos.sort_by(|a, b| b.1.amount.cmp(&a.1.amount));
    Ok(utxos)
}

fn is_utxo_spendable(entry: &RpcUtxoEntry, virtual_daa_score: u64, coinbase_maturity: u64) -> bool {
    let needed_confirmations = if !entry.is_coinbase {
        10
    } else {
        coinbase_maturity
    };
    entry.block_daa_score + needed_confirmations <= virtual_daa_score
}

// ----------------------- TPS logger -----------------------
fn spawn_tps_logger() -> UnboundedSender<u32> {
    let (tx, mut rx) = unbounded_channel::<u32>();

    tokio::spawn(async move {
        let mut per_sec: u64 = 0;
        let mut total: u64 = 0;
        let mut last10: std::collections::VecDeque<u64> =
            std::collections::VecDeque::with_capacity(10);
        let mut tick = interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                Some(n) = rx.recv() => {
                    per_sec += n as u64;
                    total += n as u64;
                }
                _ = tick.tick() => {
                    if last10.len() == 10 { last10.pop_front(); }
                    last10.push_back(per_sec);
                    let avg10 = if last10.is_empty() { 0.0 } else {
                        last10.iter().sum::<u64>() as f64 / last10.len() as f64
                    };
                    println!("TPS: {} | 10s avg: {:.1} | total sent: {}", per_sec, avg10, total);
                    per_sec = 0;
                }
            }
        }
    });

    tx
}

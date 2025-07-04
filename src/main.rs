use axum::{    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use dashmap::DashMap;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};
use reqwest::Error;
use tokio::net::TcpListener;

// region: --- Data Structures
//区块里面的交易
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TransactionStub {
    transaction_hash: String,
}

// 完成交易信息
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Transaction {
    _id: Option<String>,
    block_hash: String,
    block_number: String,
    event_root_hash: String,
    events: Vec<Event>,
    gas_used: String,
    state_root_hash: String,
    status: String,
    timestamp: u64,
    transaction_global_index: u64,
    transaction_hash: String,
    transaction_index: u32,
    transaction_type: String,
    user_transaction: UserTransaction,
}
//事件
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Event {
    _id: Option<String>,
    block_hash: String,
    block_number: String,
    data: String,
    decode_event_data: Option<String>,
    event_index: u32,
    event_key: String,
    event_seq_number: String,
    transaction_global_index: u64,
    transaction_hash: String,
    transaction_index: u32,
    type_tag: String,
}

//
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct UserTransaction {
    authenticator: Authenticator,
    raw_txn: RawTxn,
    transaction_hash: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Authenticator {
    #[serde(rename = "Ed25519")]
    ed25519: Option<Ed25519>, //有的交易这个是空的
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Ed25519 {
    public_key: String,
    signature: String,
}


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RawTxn {
    chain_id: u8,
    decoded_payload: String,
    expiration_timestamp_secs: String,
    gas_token_code: String,
    gas_unit_price: String,
    max_gas_amount: String,
    payload: String,
    sender: String,
    sequence_number: String,
    transaction_hash: String,
}

//区块
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Block {
    header: BlockHeader,
    uncles: Vec<serde_json::Value>,
    body: BlockBody,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BlockHeader {
    author: String,
    author_auth_key: Option<String>,
    block_hash: String,
    body_hash: String,
    chain_id: u8,
    nonce: u64,
    timestamp: u64,
    difficulty: String,
    extra: String,
    gas_used: u64,
    number: u64,
    parent_hash: String,
    state_root: String,
    txn_accumulator_root: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BlockBody {
    #[serde(rename = "Full")]
    full: Vec<TransactionStub>,
}

//  App State
#[derive(Clone)]
struct AppState {
    blocks: DashMap<u64, Block>,
    transactions: DashMap<String, Transaction>,
}

const API_BASE_URL: &str = "https://doapi.stcscan.io/v2";
const BLOCKS_TO_FETCH: u64 = 20000;



#[tokio::main]
async fn main() {

    // 请用rust实现一个工程实现获取starcoin 主网前2w个区块，输入属于这个区块内的transaction hash_id,
    // 不需要实现界面，返回交易相关的信息
    //大概思路，
    // 1，目前先启动的时候把所有2w个区块扫描下来，
    //2，从1开始扫描，扫描拿到区块的数据后，如果有交易，再一个一个的取交易。如果处理存量数据，可以开多线程处理，这里简单用单线程。但是处理比较慢，自己测试扫描了2000个区块，可以正常
    // 3，保存到map里面，正常应该是数据库


    //测试，
    //数据单线程扫描慢，测试用扫描了2000个区块，可以通过接口查到数据，
    //启动项目后，会先缓存数据，之后会启动3006端口，
    // 查询区块，curl http://localhost:3006/block/100
    // 查询交易：http://localhost:3006/transaction/0x61178cd278769e99c151f0f19f3f19f5d1230395c7e0026aa297f2624858780c
    //我本地启动，处理2000个区块作为测试，都能正常获取到数据
    let blocks_cache = DashMap::<u64, Block>::new();
    let transactions_cache = DashMap::<String, Transaction>::new();

    println!(
        "Starting to fetch and cache data for {} blocks...",
        BLOCKS_TO_FETCH
    );
    let start_time = Instant::now();

    //从1开始取交易，
    for block_height in 1..BLOCKS_TO_FETCH {

        let block_res = fetch_block(block_height).await;
        match block_res {
            Ok(block) => {
                let tx_hashes: Vec<String> = block
                    .body
                    .full
                    .iter()
                    .map(|t| t.transaction_hash.clone())
                    .collect();

                //取出所有交易hash，再查交易详细
                for tx_hash in tx_hashes.iter() {

                    let tx_res = fetch_transaction(&tx_hash).await;
                    match tx_res {
                        Ok(tx) => {
                            transactions_cache.insert((&tx_hash).to_string(), tx);
                        }
                        Err(txErr) => {
                            println!(
                                "fetch transaction hash {},error {}",
                                tx_hash,
                                txErr
                            );
                            return;
                        }
                    }
                }
                blocks_cache.insert(block_height, block);
            }
            Err(err) => {
                println!(
                    "fetch block height {},error {}",
                    block_height,
                    err
                );
                return;
            }
        }

    }

    let duration = start_time.elapsed();
    println!(
        "Finished caching data. Blocks: {}, Transactions: {}. Time taken: {:.2?}",
        blocks_cache.len(),
        transactions_cache.len(),
        duration
    );

    let app_state = AppState {
        blocks: blocks_cache,
        transactions: transactions_cache,
    };

    let app = Router::new()
        .route("/block/:block_height", get(get_block_handler))
        .route("/transaction/:txn_hash", get(get_transaction_handler))
        .with_state(app_state);

    let listener = TcpListener::bind("0.0.0.0:3006").await.unwrap();
    println!("\nListening on http://0.0.0.0:3006");
    println!("Try accessing:");
    println!("  http://localhost:3006/block/100");
    println!("  http://localhost:3006/transaction/0x4a453050784e2cb8d52907b37b262b7def83e4a1b581dac9abbb866be72612d0");
    axum::serve(listener, app).await.unwrap();
}

// 根据区块号查区块，会超时或者失败，加了重拾10次，如果还不行，那就是结构定义不对
async fn fetch_block(block_height: u64) -> Result<Block, reqwest::Error> {
    for n in 1..10 {
        let url = format!("{}/block/main/height/{}", API_BASE_URL, block_height);
        let response = reqwest::get(&url).await;
        match response {
            Ok(res) => {
                return res.json::< crate::Block >().await
            }
            Err(err) => {
                println!("fetch block error {}",err);
                // sleep(Duration::from_millis(2))
            }
        }
    }
    panic!("fetch block error")

}

// 交易hash查查询交易，会超时或者失败，加了重拾10次，如果还不行，那就是结构定义不对
async fn fetch_transaction(txn_hash: &str) -> Result<Transaction, reqwest::Error> {
    for n in 1..10 {
        let url = format!("{}/transaction/main/hash/{}", API_BASE_URL, txn_hash);
        let response = reqwest::get(&url).await;
        match response {
            Ok(res) => {
                return res.json::< crate::Transaction >().await
            }
            Err(err) => {
                println!("fetch transaction error {}",err);

            }
        }
    }

    panic!("fetch transaction error")


}
//  API Client
//查询区块
async fn get_block_handler(
    State(state): State<AppState>,
    Path(block_height): Path<u64>,
) -> Result<Json<Block>, StatusCode> {
    match state.blocks.get(&block_height) {
        Some(block) => Ok(Json(block.clone())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

//查询交易
async fn get_transaction_handler(
    State(state): State<AppState>,
    Path(txn_hash): Path<String>,
) -> Result<Json<Transaction>, StatusCode> {
    match state.transactions.get(&txn_hash) {
        Some(txn) => Ok(Json(txn.clone())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

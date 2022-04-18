use std::{collections::HashMap, hash::Hash};
use rust_decimal::{Decimal, prelude::Zero};
use ftx::ws;
use ftx::ws::{Channel, Data, Orderbook, Ws};
use ftx::{
    options::Options,
    rest::{PlaceOrder, Side, OrderType, Rest},
};
use futures::stream::StreamExt;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> ws::Result<()> {
    dotenv::dotenv().ok();

    let mut websocket = Ws::connect(Options::from_env()).await?;
    let api = Rest::new(Options::from_env());

    let solbtc = String::from("SOL/BTC");
    let solusd = String::from("SOL/USD");
    let solusdt = String::from("SOL/USDT");
    let fttbtc = String::from("FTT/BTC");
    let fttusd = String::from("FTT/USD");
    let fttusdt = String::from("FTT/USDT");
    let btcusd = String::from("BTC/USD");
    let btcusdt = String::from("BTC/USDT");
    let usdtusd = String::from("USDT/USD");

    let mut solbtcorderbook = Orderbook::new(solbtc.to_owned());
    let mut solusdorderbook = Orderbook::new(solusd.to_owned());
    let mut solusdtorderbook = Orderbook::new(solusdt.to_owned());
    let mut fttbtcorderbook = Orderbook::new(fttbtc.to_owned());
    let mut fttusdorderbook = Orderbook::new(fttusd.to_owned());
    let mut fttusdtorderbook = Orderbook::new(fttusdt.to_owned());
    let mut btcusdorderbook = Orderbook::new(btcusd.to_owned());
    let mut btcusdtorderbook = Orderbook::new(btcusdt.to_owned());
    let mut usdtusdorderbook = Orderbook::new(usdtusd.to_owned());

    websocket.subscribe(vec![
        Channel::Orderbook(solbtc.to_owned()),
        Channel::Orderbook(solusd.to_owned()),
        Channel::Orderbook(solusdt.to_owned()),
        Channel::Orderbook(fttbtc.to_owned()),
        Channel::Orderbook(fttusd.to_owned()),
        Channel::Orderbook(fttusdt.to_owned()),
        Channel::Orderbook(btcusd.to_owned()),
        Channel::Orderbook(btcusdt.to_owned()),
        Channel::Orderbook(usdtusd.to_owned()),
    ]).await?;

    let (tx, mut rx) = mpsc::channel(100);

    tokio::spawn(async move {
        loop {
            let data = websocket.next().await.expect("No data received").unwrap();

            tx.send(data).await.unwrap();
        }
    });

    
    let mut graph = Graph::new();
    graph.add_vertex("SOL".to_string());
    graph.add_vertex("FTT".to_string());
    graph.add_vertex("BTC".to_string());
    graph.add_vertex("USD".to_string());
    graph.add_vertex("USDT".to_string());

    let zero = Decimal::zero();
    let one = Decimal::from(1_u8);
    
    graph.add_edge("SOL".to_string(), "BTC".to_string(), zero, zero);
    graph.add_edge("BTC".to_string(), "SOL".to_string(), zero, zero);
    graph.add_edge("SOL".to_string(), "USD".to_string(), zero, zero);
    graph.add_edge("USD".to_string(), "SOL".to_string(), zero, zero);
    graph.add_edge("SOL".to_string(), "USDT".to_string(), zero, zero);
    graph.add_edge("USDT".to_string(), "SOL".to_string(), zero, zero);
    graph.add_edge("FTT".to_string(), "BTC".to_string(), zero, zero);
    graph.add_edge("BTC".to_string(), "FTT".to_string(), zero, zero);
    graph.add_edge("FTT".to_string(), "USD".to_string(), zero, zero);
    graph.add_edge("USD".to_string(), "FTT".to_string(), zero, zero);
    graph.add_edge("FTT".to_string(), "USDT".to_string(), zero, zero);
    graph.add_edge("USDT".to_string(), "FTT".to_string(), zero, zero);
    graph.add_edge("BTC".to_string(), "USD".to_string(), zero, zero);
    graph.add_edge("USD".to_string(), "BTC".to_string(), zero, zero);
    graph.add_edge("BTC".to_string(), "USDT".to_string(), zero, zero);
    graph.add_edge("USDT".to_string(), "BTC".to_string(), zero, zero);
    graph.add_edge("USDT".to_string(), "USD".to_string(), zero, zero);
    graph.add_edge("USD".to_string(), "USDT".to_string(), zero, zero);


    while let Some(obdata) = rx.recv().await {
        match obdata {
            (opsymbol, Data::OrderbookData(orderbook_data)) => {
                match opsymbol {
                    Some(symbol) => {
                        match symbol.as_str() {
                            "SOL/BTC" => {
                                solbtcorderbook.update(&orderbook_data);
                                let (askprice, askquantity) = solbtcorderbook.best_ask().unwrap();
                                graph.update_price_quantity("SOL".to_string(), "BTC".to_string(), fees(askprice, false), askprice * askquantity);
                                let (bidprice, bidquantity) = solbtcorderbook.best_bid().unwrap();
                                graph.update_price_quantity("BTC".to_string(), "SOL".to_string(), fees(one/bidprice, true), bidquantity);
                            },
                            "SOL/USD" => {
                                solusdorderbook.update(&orderbook_data);
                                let (askprice, askquantity) = solusdorderbook.best_ask().unwrap();
                                graph.update_price_quantity("SOL".to_string(), "USD".to_string(), fees(askprice, false), askprice * askquantity);
                                let (bidprice, bidquantity) = solusdorderbook.best_bid().unwrap();
                                graph.update_price_quantity("USD".to_string(), "SOL".to_string(), fees(one/bidprice, true), bidquantity);
                            },
                            "SOL/USDT" => {
                                solusdtorderbook.update(&orderbook_data);
                                let (askprice, askquantity) = solusdtorderbook.best_ask().unwrap();
                                graph.update_price_quantity("SOL".to_string(), "USDT".to_string(), fees(askprice, false), askprice * askquantity);
                                let (bidprice, bidquantity) = solusdtorderbook.best_bid().unwrap();
                                graph.update_price_quantity("USDT".to_string(), "SOL".to_string(), fees(one/bidprice, true), bidquantity);
                            },
                            "FTT/BTC" => {
                                fttbtcorderbook.update(&orderbook_data);
                                let (askprice, askquantity) = fttbtcorderbook.best_ask().unwrap();
                                graph.update_price_quantity("FTT".to_string(), "BTC".to_string(), fees(askprice, false), askprice * askquantity);
                                let (bidprice, bidquantity) = fttbtcorderbook.best_bid().unwrap();
                                graph.update_price_quantity("BTC".to_string(), "FTT".to_string(), fees(one/bidprice, true), bidquantity);
                            },
                            "FTT/USD" => {
                                fttusdorderbook.update(&orderbook_data);
                                let (askprice, askquantity) = fttusdorderbook.best_ask().unwrap();
                                graph.update_price_quantity("FTT".to_string(), "USD".to_string(), fees(askprice, false), askprice * askquantity);
                                let (bidprice, bidquantity) = fttusdorderbook.best_bid().unwrap();
                                graph.update_price_quantity("USD".to_string(), "FTT".to_string(), fees(one/bidprice, true), bidquantity);
                            },
                            "FTT/USDT" => {
                                fttusdtorderbook.update(&orderbook_data);
                                let (askprice, askquantity) = fttusdtorderbook.best_ask().unwrap();
                                graph.update_price_quantity("FTT".to_string(), "USDT".to_string(), fees(askprice, false), askprice * askquantity);
                                let (bidprice, bidquantity) = fttusdtorderbook.best_bid().unwrap();
                                graph.update_price_quantity("USDT".to_string(), "FTT".to_string(), fees(one/bidprice, true), bidquantity);
                            },
                            "BTC/USD" => {
                                btcusdorderbook.update(&orderbook_data);
                                let (askprice, askquantity) = btcusdorderbook.best_ask().unwrap();
                                graph.update_price_quantity("BTC".to_string(), "USD".to_string(), fees(askprice, false), askprice * askquantity);
                                let (bidprice, bidquantity) = btcusdorderbook.best_bid().unwrap();
                                graph.update_price_quantity("USD".to_string(), "BTC".to_string(), fees(one/bidprice,true), bidquantity);
                            },
                            "BTC/USDT" => {
                                btcusdtorderbook.update(&orderbook_data);
                                let (askprice, askquantity) = btcusdtorderbook.best_ask().unwrap();
                                graph.update_price_quantity("BTC".to_string(), "USDT".to_string(), fees(askprice, false), askprice * askquantity);
                                let (bidprice, bidquantity) = btcusdtorderbook.best_bid().unwrap();
                                graph.update_price_quantity("USDT".to_string(), "BTC".to_string(), fees(one/bidprice, true), bidquantity);
                            },
                            "USDT/USD" => {
                                usdtusdorderbook.update(&orderbook_data);
                                let (askprice, askquantity) = usdtusdorderbook.best_ask().unwrap();
                                graph.update_price_quantity("USDT".to_string(), "USD".to_string(), fees(askprice, false), askprice * askquantity);
                                let (bidprice, bidquantity) = usdtusdorderbook.best_bid().unwrap();
                                graph.update_price_quantity("USD".to_string(), "USDT".to_string(), fees(one/bidprice, true), bidquantity);
                            },
                            _ => {
                                panic!("Unexpected symbol in orderbook update: {}", symbol);
                            }

                        }
                    },
                    None => {}
                }
            }

            _ => panic!("Unexpected data received: {:?}", obdata),
        }
        let usd = String::from("USD");
        let (profitable, amounts, path) = find_arbitrage(&graph, &usd);
        if profitable {
            println!("{:?}", path);
            println!("{:?}", amounts);
            // let endamt = amounts.last().unwrap().1;
            for (coin_a, coin_b) in path {
                
            };
        // } else {

        };
    }
   
    Ok(())
}

// graph node
pub struct Graph<VId> {
    vertices: Vec<VId>,
    edges: HashMap<VId, Vec<(VId, Decimal, Decimal)>>,
}

impl<VId> Graph<VId> 
    where
    VId: Eq + Hash,
{
    pub fn new() -> Self {
        Graph {
            vertices: Vec::new(),
            edges: HashMap::new(),
        }
    }

    pub fn add_vertex(&mut self, vertex: VId) {
        self.vertices.push(vertex);
    }

    pub fn add_edge(&mut self, from: VId, to: VId, price: Decimal, quantity: Decimal) {
        self.edges.entry(from).or_default().push((to, price, quantity));
    } 

    pub fn update_price_quantity(&mut self, from: VId, to: VId, price: Decimal, quantity: Decimal) {
        let edges = self.edges.entry(from).or_default();
        for edge in edges.iter_mut() {
            if edge.0 == to {
                edge.1 = price;
                edge.2 = quantity;
                break;
            }
        }
    }
}
/* 
//find negative cycle
fn bellman_ford<'a >(graph: &'a Graph<&str>, start: &'a str) -> (bool, Decimal, Vec<(&'a &'a str, &'a &'a str)>) {
    let mut distance = HashMap::new();
    let zero = Decimal::zero();

    //initialize all distances to 0
    for vertex in &graph.vertices {
        distance.insert(vertex, zero);
    }
    
    //initalizes start to 1
    distance.insert(&start, Decimal::from(1_u8));

    let mut predecessors = Vec::new();
    
    let mut profitable = false;

    'outer: for _ in 0..graph.vertices.len() {
        for (vertex, edges) in graph.edges.iter() {
            for (to, weight) in edges {
                //if the "value" after the edge traversal is greater than current "value" of the vertex, update
                if (distance[vertex] * weight) > distance[to] {
                    distance.insert(to, distance[vertex] * weight);
                    predecessors.push((to, vertex));

                    //if you can end up with more than started, there is a profitable cycle
                    if to == &start {
                        profitable = true;
                        break 'outer;
                    }
                }
            }
        }
    }

    println!("{:?}", profitable);
    println!("{:?}", distance);
    println!("{:?}", predecessors);

    if profitable {
        return (true, distance[&start]-Decimal::from(1_u8), predecessors);
    } else {
        return (false, zero, predecessors);
    }

}
*/

fn find_arbitrage<'a>(graph: &'a Graph<String>, start: &'a String) -> (bool, Vec<(&'a String, Decimal)>, Vec<(&'a String, &'a String)>) {
    let mut distance = HashMap::new();
    let zero = Decimal::zero();

    //initialize all distances to 0
    for vertex in &graph.vertices {
        distance.insert(vertex, zero);
    }
    
    //initalizes start to 1
    distance.insert(&start, Decimal::from(1_u8));

    let mut predecessors = Vec::new();
    let mut max_amts = Vec::new();
    let mut profitable = false;

    'outer: for (to0, weight0, amt0) in &graph.edges[start] {
        distance.insert(&to0, *weight0);
        predecessors.push((start, to0));

        for (to1, weight1, amt1) in &graph.edges[to0] {
            distance.insert(&to1, *weight1 * *weight0);
            predecessors.push((to0, to1));
            let min1 = std::cmp::min(*amt1, amt0 * weight1);

            if distance[&start] > Decimal::from(1_u8) {
                profitable = true;
                max_amts.push((to0, min1 / weight0 / weight1));
                max_amts.push((to1, min1));
                break 'outer;
            }

            for (to2, weight2, amt2) in &graph.edges[to1] {
                distance.insert(&to2, *weight2 * *weight1 * *weight0);
                predecessors.push((to1, to2));
                let min2 = std::cmp::min(*amt2, min1 * weight2);

                if distance[&start] > Decimal::from(1_u8) {
                    profitable = true;
                    max_amts.push((to0, min2 / weight0 / weight1 / weight2));
                    max_amts.push((to1, min2 / weight1 / weight2));
                    max_amts.push((to2, min2));
                    break 'outer;
                }

                for (to3, weight3, amt3) in &graph.edges[to2] {
                    distance.insert(&to3, *weight3 * *weight2 * *weight1 * *weight0);
                    predecessors.push((to2, to3));
                    let min3 = std::cmp::min(*amt3, min2 * weight3);

                    if distance[&start] > Decimal::from(1_u8) {
                        profitable = true;
                        max_amts.push((to0, min3 / *weight0 / *weight1 / *weight2 / *weight3));
                        max_amts.push((to1, min3 / *weight1 / *weight2 / *weight3));
                        max_amts.push((to2, min3 / *weight2 / *weight3));
                        max_amts.push((to3, min3));
                        break 'outer;
                    }

                    predecessors.pop();
                    distance.insert(&to3, zero);
                }

                predecessors.pop();
                distance.insert(&to2, zero);
            }

            predecessors.pop();
            distance.insert(&to1, zero);
        }

        predecessors.pop();
        distance.insert(&to0, zero);
    }

    // println!("{:?}", profitable);
    // println!("{:?}", distance);
    // println!("{:?}", predecessors);

    

    if profitable {
        // let profit = distance[&start]-Decimal::from(1_u8);
        return (true, max_amts, predecessors.clone());
    } else {
        return (false, max_amts, predecessors.clone());
    }

}

fn fees (input: Decimal, subtract: bool) -> Decimal {
    if subtract {
    return input - (input * Decimal::from_i128_with_scale(8, 2))
    } else {
    return input + (input * Decimal::from_i128_with_scale(8, 2))
    }
}   

use chrono::DateTime;
use mysql::prelude::*;
use mysql::*;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use futures::executor::block_on;

mod parser;

#[derive(Deserialize, Debug)]
struct Root {
    contributors: Contributors,
    txns: Txns,
    campaign: Value,
}

#[derive(Deserialize, Debug)]
struct Txns {
    list: Vec<Payment>,
}

#[derive(Deserialize, Debug)]
struct Contributors {
    map: HashMap<String, FullName>,
}

#[derive(Deserialize, Debug)]
struct FullName {
    full_name: String,
}

#[derive(Deserialize, Debug)]
struct Payment {
    date: DateTime<chrono::Utc>,
    amount: f64,
    contributor_id: String,
    id: String,
}

#[derive(Debug)]
struct Contributor {
    contributor_id: String,
    full_name: String,
}


async fn get_json(url: &str) -> Result<String, reqwest::Error> {

    println!("Parsing the moneypoolinformations.");

    let res = reqwest::get(url).await?;

    let body = res.text().await?;

    let document = Html::parse_document(&body);
    
    //<script type="application/json" id="store">
    let selector = Selector::parse(r#"script[id="store"#).unwrap();
    let fragment = document.select(&selector).next().unwrap();
    
    Ok(fragment.inner_html())
}

#[tokio::main]
async fn main() {

    let mut payments: Vec<Payment> = Vec::new();
    let mut contributors: Vec<Contributor> = Vec::new();

    let pool_id = "";
    let paypal_url = "";
    let mysql_url = "";

    let future = parser::get_json(paypal_url);
    let json = block_on(future).unwrap();

    let root = serde_json::from_str::<Root>(&json).unwrap();

    //get the creator data
    let owner_contributor_id = root.campaign[pool_id]["owner"]["id"].as_str().unwrap();
    let owner_full_name = root.campaign[pool_id]["owner"]["full_name"].to_string();

    contributors.push(Contributor {
        contributor_id: owner_contributor_id.to_string(),
        full_name: owner_full_name,
    });

    let mut owner_amount = root.campaign[pool_id]["pledge"].as_f64().unwrap();

    //connect to mysql
    let pool = Pool::new(mysql_url).unwrap();
    let mut conn = pool.get_conn().unwrap();

    //create tables if not exist
    conn.query_drop(
        r"CREATE TABLE IF NOT EXISTS payments (
                id VARCHAR(30),
                date DATE,
                amount DOUBLE,
                contributor_id VARCHAR(30),
                UNIQUE KEY unique_id (id)
        )",
    )
    .unwrap();

    conn.query_drop(
        r"CREATE TABLE IF NOT EXISTS contributors (
            contributor_id VARCHAR(30) PRIMARY KEY,
            full_name VARCHAR(30)
        )",
    )
    .unwrap();

    //get amount owner has payed till today
    let mut owner_sum = 0.0;

    conn.exec_iter(
        "SELECT amount, contributor_id as a from payments where contributor_id = :owner_contributor_id",
        params! {owner_contributor_id},
    )
    .unwrap()
    .for_each(|row| {
        let r: (f64, String) = from_row(row.unwrap());
        owner_sum = owner_sum + r.0;
    });

    if owner_amount > owner_sum {
        owner_amount = owner_amount - owner_sum;
        payments.push(Payment {
            date: chrono::offset::Utc::now(),
            amount: owner_amount,
            contributor_id: owner_contributor_id.to_string(),
            id: chrono::offset::Utc::now().timestamp().to_string() + owner_contributor_id,
        });
    }

    //write json to payment and contributors
    for elem in root.txns.list {
        payments.push(Payment {
            date: elem.date,
            amount: elem.amount,
            contributor_id: elem.contributor_id.as_str().to_string(),
            id: elem.date.timestamp().to_string() + elem.contributor_id.as_str(),
        });
    }

    for (key, value) in root.contributors.map {
        contributors.push(Contributor {
            contributor_id: key,
            full_name: value.full_name,
        });
    }

    conn.exec_batch(
        r"INSERT IGNORE INTO contributors (contributor_id, full_name)
          VALUES (:contributor_id, :full_name)",
        contributors.iter().map(|c| {
            params! {
                "contributor_id" => c.contributor_id.as_str(),
                "full_name" => c.full_name.as_str(),
            }
        }),
    )
    .unwrap();

    conn.exec_batch(
        r"INSERT IGNORE INTO payments (id, date, amount, contributor_id)
          VALUES (:id, :date, :amount, :contributor_id)",
        payments.iter().map(|p| {
            params! {
                "id" => p.id.as_str(),
                "date" => p.date.naive_utc(),
                "amount" => p.amount,
                "contributor_id" => p.contributor_id.as_str(),
            }
        }),
    )
    .unwrap();

    println!("Import done.");

}